output "bucket_name" {
  description = "Raw-layer GCS bucket name."
  value       = google_storage_bucket.raw.name
}

output "bucket_url" {
  description = "gs:// URL of the raw-layer bucket."
  value       = google_storage_bucket.raw.url
}

output "dataset_id" {
  description = "BigQuery dataset ID."
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "fact_table_id" {
  description = "Fully qualified fact table, ready to paste into a query."
  value       = "${var.project_id}.${google_bigquery_dataset.analytics.dataset_id}.${google_bigquery_table.fct_market_observation.table_id}"
}

output "watermark_table_id" {
  description = "Fully qualified watermark table."
  value       = "${var.project_id}.${google_bigquery_dataset.analytics.dataset_id}.${google_bigquery_table.pipeline_watermark.table_id}"
}

output "service_account_email" {
  description = "Pipeline service account. Impersonate this identity rather than creating a key."
  value       = google_service_account.pipeline.email
}

output "pipeline_env" {
  description = "Shell exports the pipeline CLI expects. Run: terraform output -raw pipeline_env"
  value       = <<-EOT
    export GCP_PROJECT_ID=${var.project_id}
    export GCP_REGION=${var.region}
    export GCS_BUCKET=${google_storage_bucket.raw.name}
    export BQ_DATASET=${google_bigquery_dataset.analytics.dataset_id}
    export BQ_LOCATION=${var.bq_location}
  EOT
}
