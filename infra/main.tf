# Minimal GCP wiring for dbt BigQuery prod (see ../medallion/profiles.yml).
# Terraform does not manage: local DuckDB, dbt models, or ingest Python jobs.

provider "google" {
  project = var.project_id
  region  = var.google_default_region
}

resource "google_bigquery_dataset" "dbt" {
  dataset_id                 = var.dbt_dataset_id
  friendly_name              = "dbt medallion"
  description                = "Production relations built by dbt"
  location                   = var.bigquery_location
  delete_contents_on_destroy = false
}

resource "google_service_account" "dbt" {
  account_id   = var.dbt_service_account_id
  display_name = "dbt medallion"
  description  = "Runs dbt against BigQuery (job user + dataset data editor)"
}

# Required to submit BigQuery jobs (CREATE VIEW, queries, etc.).
resource "google_project_iam_member" "dbt_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

# Read/write objects in the dbt dataset only (tightest common default for a single prod dataset).
resource "google_bigquery_dataset_iam_member" "dbt_editor" {
  dataset_id = google_bigquery_dataset.dbt.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

# Optional: allow reading other datasets (e.g. raw landing zone) by adding more
# google_bigquery_dataset_iam_member or project-level IAM here.
