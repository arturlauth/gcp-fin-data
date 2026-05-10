terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS (Landing) ─────────────────────────────────────────────────────────────

resource "google_storage_bucket" "landing" {
  name                        = var.gcs_bucket_name
  location                    = "US"
  force_destroy               = false
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }
}

# ── BigQuery ──────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = "US"
}

resource "google_bigquery_dataset" "trusted" {
  dataset_id = "trusted"
  location   = "US"
}

resource "google_bigquery_dataset" "refined" {
  dataset_id = "refined"
  location   = "US"
}

resource "google_bigquery_dataset" "governance" {
  dataset_id  = "governance"
  location    = "US"
  description = "Pipeline audit logs and operational metadata"
}

resource "google_bigquery_table" "ingestion_log" {
  dataset_id          = google_bigquery_dataset.governance.dataset_id
  table_id            = "ingestion_log"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "_log_date"
  }

  schema = jsonencode([
    { name = "log_id",         type = "STRING",    mode = "REQUIRED" },
    { name = "project_id",     type = "STRING",    mode = "REQUIRED" },
    { name = "pipeline",       type = "STRING",    mode = "REQUIRED" },
    { name = "layer",          type = "STRING",    mode = "REQUIRED" },
    { name = "entity",         type = "STRING",    mode = "REQUIRED" },
    { name = "frequency",      type = "STRING",    mode = "NULLABLE" },
    { name = "reference_date", type = "DATE",      mode = "NULLABLE" },
    { name = "record_count",   type = "INTEGER",   mode = "NULLABLE" },
    { name = "blob_path",      type = "STRING",    mode = "NULLABLE" },
    { name = "bq_table",       type = "STRING",    mode = "NULLABLE" },
    { name = "status",         type = "STRING",    mode = "REQUIRED" },
    { name = "error_message",  type = "STRING",    mode = "NULLABLE" },
    { name = "ingested_at",    type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "_log_date",      type = "DATE",      mode = "REQUIRED" },
  ])

  lifecycle {
    ignore_changes = [schema]
  }
}

resource "google_bigquery_table" "raw_btcbrl_trades" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "btcbrl_trades"
  deletion_protection = false

  time_partitioning {
    type          = "DAY"
    field         = "_load_date"
    expiration_ms = 7776000000 # 90 days — matches GCS lifecycle
  }

  schema = jsonencode([
    { name = "payload",      type = "STRING"    },
    { name = "_source_file", type = "STRING"    },
    { name = "_ingested_at", type = "TIMESTAMP" },
    { name = "_load_date",   type = "DATE"      },
  ])
}

# trusted.* tables are owned by dbt — not defined in Terraform

# ── Artifact Registry ─────────────────────────────────────────────────────────

resource "google_artifact_registry_repository" "cloud_run" {
  location      = "us-central1"
  repository_id = "cloud-run"
  format        = "DOCKER"
}

# ── Service Accounts ──────────────────────────────────────────────────────────

resource "google_service_account" "streamer" {
  account_id   = "sa-streamer"
  display_name = "Streamer VM"
}

resource "google_service_account" "pipeline" {
  account_id   = "sa-pipeline"
  display_name = "Pipeline Jobs"
}

# ── IAM ───────────────────────────────────────────────────────────────────────

resource "google_storage_bucket_iam_member" "streamer_landing_writer" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.streamer.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_landing_reader" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bq_data_owner" {
  project = var.project_id
  role    = "roles/bigquery.dataOwner"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloudfunctions2_function_iam_member" "pipeline_cf_invoker" {
  project        = var.project_id
  location       = var.region
  cloud_function = google_cloudfunctions2_function.landing_to_raw.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_run_v2_service_iam_member" "pipeline_landing_to_raw_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.landing_to_raw.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline.email}"
}

# ── Streamer VM ───────────────────────────────────────────────────────────────

resource "google_storage_bucket_object" "streamer_app" {
  name   = "streamer-source/app.py"
  bucket = google_storage_bucket.landing.name
  source = "${path.module}/../../jobs/0_landing/vm_binance_btcbrl/app.py"
}

resource "google_storage_bucket_object" "streamer_requirements" {
  name   = "streamer-source/requirements.txt"
  bucket = google_storage_bucket.landing.name
  source = "${path.module}/../../jobs/0_landing/vm_binance_btcbrl/requirements.txt"
}

resource "google_compute_instance" "streamer" {
  name         = "streamer"
  machine_type = "e2-micro"
  zone         = "${var.region}-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 10
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  service_account {
    email  = google_service_account.streamer.email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = templatefile("${path.module}/streamer_startup.sh.tpl", {
    project_id      = var.project_id
    gcs_bucket      = google_storage_bucket.landing.name
    binance_streams = var.binance_streams
  })

  depends_on = [
    google_storage_bucket_object.streamer_app,
    google_storage_bucket_object.streamer_requirements,
  ]
}

# ── Cloud Function: landing → raw ─────────────────────────────────────────────

data "archive_file" "landing_to_raw" {
  type        = "zip"
  source_dir  = "${path.module}/../../jobs/1_raw/cf_binance_btcbrl"
  output_path = "${path.module}/landing_to_raw.zip"
}

resource "google_storage_bucket_object" "landing_to_raw_source" {
  name   = "cf-source/landing-to-raw-${data.archive_file.landing_to_raw.output_md5}.zip"
  bucket = google_storage_bucket.landing.name
  source = data.archive_file.landing_to_raw.output_path
}

resource "google_cloudfunctions2_function" "landing_to_raw" {
  name     = "landing-to-raw"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "binance_btcbrl"
    environment_variables = {
      GOOGLE_FUNCTION_SOURCE = "binance_btcbrl.py"
    }
    source {
      storage_source {
        bucket = google_storage_bucket.landing.name
        object = google_storage_bucket_object.landing_to_raw_source.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.pipeline.email
    timeout_seconds       = 540
    environment_variables = {
      GCP_PROJECT_ID     = var.project_id
      GCS_BUCKET         = google_storage_bucket.landing.name
      GCS_LANDING_PREFIX = "landing/binance/btcbrl_trades"
    }
  }
}

resource "google_cloud_scheduler_job" "landing_to_raw_daily" {
  name      = "landing-to-raw-daily"
  region    = var.region
  schedule  = "0 2 * * *" # 02:00 UTC — after midnight, full previous day available
  time_zone = "UTC"

  http_target {
    uri         = google_cloudfunctions2_function.landing_to_raw.service_config[0].uri
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.pipeline.email
    }
  }
}

# ── Cloud Function: tesouro leiloes landing ───────────────────────────────────

data "archive_file" "tesouro_leiloes" {
  type        = "zip"
  source_dir  = "${path.module}/../../jobs/0_landing/cf_tesouro_leiloes"
  output_path = "${path.module}/tesouro_leiloes.zip"
}

resource "google_storage_bucket_object" "tesouro_leiloes_source" {
  name   = "cf-source/tesouro-leiloes-${data.archive_file.tesouro_leiloes.output_md5}.zip"
  bucket = google_storage_bucket.landing.name
  source = data.archive_file.tesouro_leiloes.output_path
}

resource "google_cloudfunctions2_function" "tesouro_leiloes" {
  name     = "tesouro-leiloes"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "tesouro_leiloes"
    environment_variables = {
      GOOGLE_FUNCTION_SOURCE = "tesouro_leiloes.py"
    }
    source {
      storage_source {
        bucket = google_storage_bucket.landing.name
        object = google_storage_bucket_object.tesouro_leiloes_source.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.streamer.email
    timeout_seconds       = 540
    environment_variables = {
      GCP_PROJECT_ID     = var.project_id
      GCS_BUCKET         = google_storage_bucket.landing.name
      GCS_LANDING_PREFIX = "landing/tesouro_leiloes"
    }
  }
}

resource "google_cloudfunctions2_function_iam_member" "pipeline_tesouro_leiloes_invoker" {
  project        = var.project_id
  location       = var.region
  cloud_function = google_cloudfunctions2_function.tesouro_leiloes.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_run_v2_service_iam_member" "pipeline_tesouro_leiloes_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.tesouro_leiloes.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_scheduler_job" "tesouro_leiloes_daily" {
  name      = "tesouro-leiloes-daily"
  region    = var.region
  schedule  = "0 6 * * *"
  time_zone = "UTC"

  http_target {
    uri         = google_cloudfunctions2_function.tesouro_leiloes.service_config[0].uri
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.pipeline.email
    }
  }
}

# ── BigQuery Scheduled Query: raw → trusted ───────────────────────────────────

# DTS service agent needs to impersonate sa-pipeline to run the scheduled query
resource "google_service_account_iam_member" "dts_token_creator" {
  service_account_id = google_service_account.pipeline.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${var.project_number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}

# ── BigQuery Table: raw.tesouro_leiloes ───────────────────────────────────────

resource "google_bigquery_table" "raw_tesouro_leiloes" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "tesouro_leiloes"
  deletion_protection = false

  time_partitioning {
    type          = "DAY"
    field         = "_load_date"
    expiration_ms = 7776000000 # 90 days
  }

  schema = jsonencode([
    { name = "payload",       type = "STRING"    },
    { name = "endpoint_name", type = "STRING"    },
    { name = "_source_file",  type = "STRING"    },
    { name = "_ingested_at",  type = "TIMESTAMP" },
    { name = "_load_date",    type = "DATE"      },
  ])
}

# ── Cloud Function: tesouro leiloes landing → raw ─────────────────────────────

data "archive_file" "tesouro_leiloes_raw" {
  type        = "zip"
  source_dir  = "${path.module}/../../jobs/1_raw/cf_tesouro_leiloes"
  output_path = "${path.module}/tesouro_leiloes_raw.zip"
}

resource "google_storage_bucket_object" "tesouro_leiloes_raw_source" {
  name   = "cf-source/tesouro-leiloes-raw-${data.archive_file.tesouro_leiloes_raw.output_md5}.zip"
  bucket = google_storage_bucket.landing.name
  source = data.archive_file.tesouro_leiloes_raw.output_path
}

resource "google_cloudfunctions2_function" "tesouro_leiloes_raw" {
  name     = "tesouro-leiloes-raw"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "tesouro_leiloes"
    environment_variables = {
      GOOGLE_FUNCTION_SOURCE = "tesouro_leiloes.py"
    }
    source {
      storage_source {
        bucket = google_storage_bucket.landing.name
        object = google_storage_bucket_object.tesouro_leiloes_raw_source.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.pipeline.email
    timeout_seconds       = 540
    environment_variables = {
      GCP_PROJECT_ID     = var.project_id
      GCS_BUCKET         = google_storage_bucket.landing.name
      GCS_LANDING_PREFIX = "landing/tesouro_leiloes"
    }
  }
}

resource "google_cloudfunctions2_function_iam_member" "pipeline_tesouro_leiloes_raw_invoker" {
  project        = var.project_id
  location       = var.region
  cloud_function = google_cloudfunctions2_function.tesouro_leiloes_raw.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_run_v2_service_iam_member" "pipeline_tesouro_leiloes_raw_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.tesouro_leiloes_raw.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_scheduler_job" "tesouro_leiloes_raw_daily" {
  name      = "tesouro-leiloes-raw-daily"
  region    = var.region
  schedule  = "0 4 * * *" # 04:00 UTC — after tesouro landing CF at 02:00 UTC
  time_zone = "UTC"

  http_target {
    uri         = google_cloudfunctions2_function.tesouro_leiloes_raw.service_config[0].uri
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.pipeline.email
    }
  }
}

# ── BACEN Olinda: BigQuery raw tables ────────────────────────────────────────
# cf_bacen deprecated — only tables used by cf_bacen_olindaclient remain

locals {
  bacen_raw_tables = toset([
    "taxa_juros_mensal",
    "ifdata_lista_relatorio",
    "ifdata_cadastro",
    "meios_pagamento_mensal",
    "expectativas_anuais",
  ])
}

resource "google_bigquery_table" "bacen_raw" {
  for_each            = local.bacen_raw_tables
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "bacen_${each.key}"
  deletion_protection = false

  time_partitioning {
    type          = "DAY"
    field         = "_load_date"
    expiration_ms = 7776000000 # 90 days
  }

  schema = jsonencode([
    { name = "payload",      type = "STRING"    },
    { name = "_source_file", type = "STRING"    },
    { name = "_ingested_at", type = "TIMESTAMP" },
    { name = "_load_date",   type = "DATE"      },
  ])
}


# ── BigQuery Scheduled Query: raw → trusted ───────────────────────────────────

# raw → trusted for Binance is now handled by dbt (dbt/models/trusted/binance_btc_trades.sql)

# ── Cloud Run Job: dbt trusted ────────────────────────────────────────────────

resource "google_cloud_run_v2_job" "dbt_trusted" {
  name     = "dbt-trusted"
  location = var.region

  template {
    template {
      containers {
        image = "us-central1-docker.pkg.dev/${var.project_id}/cloud-run/dbt-trusted:latest"

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
      service_account = google_service_account.pipeline.email
      max_retries     = 1
    }
  }
}

resource "google_cloud_run_v2_job_iam_member" "pipeline_dbt_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.dbt_trusted.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_scheduler_job" "dbt_trusted_daily" {
  name      = "dbt-trusted-daily"
  region    = var.region
  schedule  = "0 7 * * *"
  time_zone = "UTC"

  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/dbt-trusted:run"
    http_method = "POST"
    oauth_token {
      service_account_email = google_service_account.pipeline.email
    }
  }
}

# ── Cloud Function: BACEN Olinda landing ──────────────────────────────────────

data "archive_file" "bacen_olinda_landing" {
  type        = "zip"
  source_dir  = "${path.module}/../../jobs/0_landing/cf_bacen_olindaclient"
  output_path = "${path.module}/bacen_olinda_landing.zip"
}

resource "google_storage_bucket_object" "bacen_olinda_landing_source" {
  name   = "cf-source/bacen-olinda-landing-${data.archive_file.bacen_olinda_landing.output_md5}.zip"
  bucket = google_storage_bucket.landing.name
  source = data.archive_file.bacen_olinda_landing.output_path
}

resource "google_cloudfunctions2_function" "bacen_olinda_landing" {
  name     = "bacen-olinda-landing"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "bacen_olinda"
    environment_variables = {
      GOOGLE_FUNCTION_SOURCE = "main.py"
    }
    source {
      storage_source {
        bucket = google_storage_bucket.landing.name
        object = google_storage_bucket_object.bacen_olinda_landing_source.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.pipeline.email
    timeout_seconds       = 540
    available_memory      = "512Mi"
    environment_variables = {
      GCP_PROJECT_ID     = var.project_id
      GCS_BUCKET         = google_storage_bucket.landing.name
      GCS_LANDING_PREFIX = "landing/bacen"
      MAX_WORKERS        = "3"
    }
  }
}

resource "google_cloudfunctions2_function_iam_member" "pipeline_bacen_olinda_landing_invoker" {
  project        = var.project_id
  location       = var.region
  cloud_function = google_cloudfunctions2_function.bacen_olinda_landing.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_run_v2_service_iam_member" "pipeline_bacen_olinda_landing_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.bacen_olinda_landing.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_scheduler_job" "bacen_olinda_landing_daily" {
  name      = "bacen-olinda-landing-daily"
  region    = var.region
  schedule  = "0 3 * * *" # 03:00 UTC daily — polls all endpoints; landing is idempotent (blob overwrite)
  time_zone = "UTC"

  http_target {
    uri         = google_cloudfunctions2_function.bacen_olinda_landing.service_config[0].uri
    http_method = "POST"
    body        = base64encode("{\"frequency\":\"all\"}")
    headers     = { "Content-Type" = "application/json" }
    oidc_token {
      service_account_email = google_service_account.pipeline.email
    }
  }
}

# ── Cloud Function: BACEN Olinda raw ─────────────────────────────────────────

data "archive_file" "bacen_olinda_raw" {
  type        = "zip"
  source_dir  = "${path.module}/../../jobs/1_raw/cf_bacen_olindaclient"
  output_path = "${path.module}/bacen_olinda_raw.zip"
}

resource "google_storage_bucket_object" "bacen_olinda_raw_source" {
  name   = "cf-source/bacen-olinda-raw-${data.archive_file.bacen_olinda_raw.output_md5}.zip"
  bucket = google_storage_bucket.landing.name
  source = data.archive_file.bacen_olinda_raw.output_path
}

resource "google_cloudfunctions2_function" "bacen_olinda_raw" {
  name     = "bacen-olinda-raw"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "bacen_olinda"
    environment_variables = {
      GOOGLE_FUNCTION_SOURCE = "main.py"
    }
    source {
      storage_source {
        bucket = google_storage_bucket.landing.name
        object = google_storage_bucket_object.bacen_olinda_raw_source.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.pipeline.email
    timeout_seconds       = 540
    environment_variables = {
      GCP_PROJECT_ID     = var.project_id
      GCS_BUCKET         = google_storage_bucket.landing.name
      GCS_LANDING_PREFIX = "landing/bacen"
    }
  }
}

resource "google_cloudfunctions2_function_iam_member" "pipeline_bacen_olinda_raw_invoker" {
  project        = var.project_id
  location       = var.region
  cloud_function = google_cloudfunctions2_function.bacen_olinda_raw.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_run_v2_service_iam_member" "pipeline_bacen_olinda_raw_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.bacen_olinda_raw.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_scheduler_job" "bacen_olinda_raw_daily" {
  name      = "bacen-olinda-raw-daily"
  region    = var.region
  schedule  = "0 5 * * *" # 05:00 UTC daily — 2 hours after landing CF; partition truncate = idempotent
  time_zone = "UTC"

  http_target {
    uri         = google_cloudfunctions2_function.bacen_olinda_raw.service_config[0].uri
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.pipeline.email
    }
  }
}
