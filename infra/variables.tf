variable "project_id" {
  type        = string
  description = "GCP project id (matches dbt prod profile project)."
}

variable "google_default_region" {
  type        = string
  description = "Regional default for the Google provider (e.g. us-central1)."
  default     = "us-central1"
}

variable "bigquery_location" {
  type        = string
  description = "BigQuery dataset location (multi-region US/EU or a single region)."
  default     = "US"
}

variable "dbt_dataset_id" {
  type        = string
  description = "Dataset where dbt creates prod relations (medallion profiles.yml dataset)."
  default     = "dbt_dio"
}

variable "dbt_service_account_id" {
  type        = string
  description = "Short account id for the dbt runner SA (no @... suffix)."
  default     = "dbt-medallion"
}
