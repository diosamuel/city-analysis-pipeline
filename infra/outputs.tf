output "bigquery_dataset_id" {
  description = "Full dataset id project:dataset"
  value       = google_bigquery_dataset.dbt.id
}

output "dbt_service_account_email" {
  description = "Use this in CI or GOOGLE_APPLICATION_CREDENTIALS after creating a key (outside Terraform is fine)."
  value       = google_service_account.dbt.email
}
