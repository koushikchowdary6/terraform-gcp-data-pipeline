locals {
  # Bucket names are globally unique, so default to one that includes the
  # project ID rather than failing an apply on a name someone else took.
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.name_prefix}-raw-${var.environment}-${var.project_id}"

  service_account_id = "${var.name_prefix}-sa-${var.environment}"

  common_labels = merge(
    {
      application = "market-data-pipeline"
      environment = var.environment
      managed_by  = "terraform"
      component   = "data-platform"
    },
    var.labels,
  )

  # Only the APIs this configuration actually needs.
  required_apis = [
    "storage.googleapis.com",
    "bigquery.googleapis.com",
  ]
}

# -----------------------------------------------------------------------------
# APIs
# -----------------------------------------------------------------------------

resource "google_project_service" "required" {
  for_each = var.enable_apis ? toset(local.required_apis) : toset([])

  project = var.project_id
  service = each.value

  # Leave the API enabled on destroy: other workloads in the project may rely
  # on it, and disabling an API is a project-wide side effect.
  disable_on_destroy = false
}

# -----------------------------------------------------------------------------
# Raw layer: Cloud Storage
# -----------------------------------------------------------------------------

resource "google_storage_bucket" "raw" {
  # checkov:skip=CKV_GCP_62:Access logging needs a second bucket and bills per
  # log object. This bucket holds public market data with no audit requirement,
  # and object writes are already attributable via batch_id in the object path.
  # Enable it before putting anything non-public here.
  name     = local.bucket_name
  project  = var.project_id
  location = var.region
  labels   = local.common_labels

  # Uniform access means permissions come from IAM only; per-object ACLs are
  # disabled, which removes the most common way a bucket is accidentally
  # made public.
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Recover from a bad overwrite without restoring from backup.
  versioning {
    enabled = true
  }

  # Raw Parquet is reproducible from the upstream feed, so it does not need to
  # be kept forever. This is the main ongoing storage cost in the project.
  dynamic "lifecycle_rule" {
    for_each = var.raw_retention_days > 0 ? [1] : []

    content {
      condition {
        age = var.raw_retention_days
      }
      action {
        type = "Delete"
      }
    }
  }

  # Clean up non-current versions created by the versioning rule above, so
  # overwritten objects do not accumulate indefinitely.
  lifecycle_rule {
    condition {
      num_newer_versions = 3
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = var.bucket_force_destroy

  depends_on = [google_project_service.required]
}

# -----------------------------------------------------------------------------
# Warehouse: BigQuery
# -----------------------------------------------------------------------------

resource "google_bigquery_dataset" "analytics" {
  # checkov:skip=CKV_GCP_81:Data at rest is already encrypted with
  # Google-managed keys. A customer-managed key (CMEK) adds a Cloud KMS key to
  # provision, rotate and pay for, which is not justified for public market
  # data. For regulated or private data, add default_encryption_configuration
  # here and kms_key_name on each table.
  dataset_id    = var.dataset_id
  project       = var.project_id
  location      = var.bq_location
  friendly_name = "Public market analytics (${var.environment})"
  description   = "Staging and analytics tables for the public-market data pipeline. Managed by Terraform."
  labels        = local.common_labels

  delete_contents_on_destroy = !var.table_deletion_protection

  depends_on = [google_project_service.required]
}

# The durable fact table is declared here rather than created by the pipeline,
# so its partitioning, clustering and schema are reviewed in version control.
# The staging table is deliberately NOT declared: it is ephemeral and the load
# job recreates it with WRITE_TRUNCATE on every run.
resource "google_bigquery_table" "fct_market_observation" {
  # checkov:skip=CKV_GCP_80:See the CMEK note on google_bigquery_dataset.analytics.
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "fct_market_observation"
  project    = var.project_id
  labels     = local.common_labels

  description = <<-EOT
    One row per (series_id, metric_name, observation_date), keyed by the
    deterministic observation_key produced during ingestion. Written by the
    MERGE in sql/bigquery/02_merge.sql.
  EOT

  # Partitioning on the query predicate that matters: almost every read is
  # bounded by date, and the MERGE prunes to the batch's date window.
  time_partitioning {
    type  = "DAY"
    field = "observation_date"
  }

  # Top-level argument: the identically named field inside time_partitioning is
  # deprecated and scheduled for removal in a future provider major version.
  require_partition_filter = var.require_partition_filter

  # Clustering keys ordered most-selective-first for the expected filters
  # (series first, then metric), which is how BigQuery prunes blocks.
  clustering = ["series_id", "metric_name"]

  schema = file("${path.module}/schemas/fct_market_observation.json")

  deletion_protection = var.table_deletion_protection
}

resource "google_bigquery_table" "pipeline_watermark" {
  # checkov:skip=CKV_GCP_80:See the CMEK note on google_bigquery_dataset.analytics.
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "pipeline_watermark"
  project    = var.project_id
  labels     = local.common_labels

  description = "Highest observation_date successfully merged per source. Drives incremental loads."

  # Tiny control table: partitioning and clustering would add cost, not save it.
  schema = file("${path.module}/schemas/pipeline_watermark.json")

  deletion_protection = var.table_deletion_protection
}

# -----------------------------------------------------------------------------
# Identity and least-privilege IAM
# -----------------------------------------------------------------------------

resource "google_service_account" "pipeline" {
  account_id   = local.service_account_id
  project      = var.project_id
  display_name = "Market data pipeline (${var.environment})"
  description  = "Runs ingestion: writes raw objects to GCS and loads/merges into BigQuery."
}

# Scoped to this bucket, not the project: the pipeline can write its own raw
# objects and nothing else in the project.
resource "google_storage_bucket_iam_member" "pipeline_object_admin" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.pipeline.member
}

# Scoped to this dataset: read/write tables and data inside it only.
resource "google_bigquery_dataset_iam_member" "pipeline_data_editor" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.pipeline.member
}

# jobUser is the one binding that cannot be scoped below the project: running
# any query or load job is a project-level permission in BigQuery. It grants
# the ability to start jobs and be billed for them, not to read any data -
# data access still comes from the dataset-scoped binding above.
resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.pipeline.member
}

# Keyless access: listed principals mint short-lived tokens for the service
# account instead of a downloadable JSON key. No key is ever created here, so
# there is nothing to leak or rotate.
resource "google_service_account_iam_member" "pipeline_impersonation" {
  for_each = toset(var.pipeline_runner_members)

  service_account_id = google_service_account.pipeline.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}
