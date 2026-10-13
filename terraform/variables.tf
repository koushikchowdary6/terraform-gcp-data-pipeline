variable "project_id" {
  description = "GCP project ID that will own the bucket, dataset and service account."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a valid GCP project ID: 6-30 characters, lowercase letters, digits and hyphens, starting with a letter."
  }
}

variable "region" {
  description = "Default region for regional resources (the GCS bucket)."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment name. Used in resource names and labels so dev and prod cannot collide."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "stage", "prod"], var.environment)
    error_message = "environment must be one of: dev, stage, prod."
  }
}

variable "name_prefix" {
  description = "Prefix applied to every resource name."
  type        = string
  default     = "market-pipeline"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.name_prefix))
    error_message = "name_prefix must be lowercase letters, digits and hyphens, starting with a letter, max 21 characters."
  }
}

variable "bucket_name" {
  description = <<-EOT
    Name of the raw-layer GCS bucket. Bucket names are globally unique, so leave
    this empty to get "<name_prefix>-raw-<environment>-<project_id>", which is
    unique by construction.
  EOT
  type        = string
  default     = ""
}

variable "dataset_id" {
  description = "BigQuery dataset ID. Must use underscores; BigQuery rejects hyphens."
  type        = string
  default     = "market_analytics"

  validation {
    condition     = can(regex("^[a-zA-Z0-9_]{1,1024}$", var.dataset_id))
    error_message = "dataset_id may contain only letters, digits and underscores."
  }
}

variable "bq_location" {
  description = "BigQuery dataset location. Cannot be changed after creation without recreating the dataset."
  type        = string
  default     = "US"
}

variable "raw_retention_days" {
  description = "Days to keep objects in the raw layer before lifecycle deletion. Set 0 to keep forever."
  type        = number
  default     = 90

  validation {
    condition     = var.raw_retention_days >= 0 && var.raw_retention_days <= 3650
    error_message = "raw_retention_days must be between 0 and 3650."
  }
}

variable "enable_apis" {
  description = <<-EOT
    Whether Terraform should enable the required Google Cloud APIs. Set false if
    the APIs are already enabled or the caller lacks serviceusage permissions
    (see README prerequisites).
  EOT
  type        = bool
  default     = true
}

variable "require_partition_filter" {
  description = <<-EOT
    Require every query against the fact table to filter on observation_date.
    This is the strongest guard against accidental full-table scans, but it also
    rejects legitimate whole-history aggregations, so it defaults to false. Turn
    it on for a shared or production dataset.
  EOT
  type        = bool
  default     = false
}

variable "table_deletion_protection" {
  description = "Block `terraform destroy` from dropping tables that contain data. Set false to allow teardown."
  type        = bool
  default     = true
}

variable "bucket_force_destroy" {
  description = "Allow `terraform destroy` to delete a bucket that still contains objects."
  type        = bool
  default     = false
}

variable "pipeline_runner_members" {
  description = <<-EOT
    Principals allowed to impersonate the pipeline service account, e.g.
    ["user:you@example.com"] or a CI workload-identity principal. This is the
    keyless alternative to downloading a service-account key: the caller mints a
    short-lived token instead. Leave empty to grant nothing.
  EOT
  type        = list(string)
  default     = []
}

variable "labels" {
  description = "Additional labels merged into every resource's labels."
  type        = map(string)
  default     = {}
}
