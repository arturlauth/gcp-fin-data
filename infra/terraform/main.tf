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

resource "google_bigquery_table" "trusted_btcbrl_trades" {
  dataset_id          = google_bigquery_dataset.trusted.dataset_id
  table_id            = "binance_btc_trades"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "trade_time"
  }

  schema = jsonencode([
    { name = "trade_id",       type = "INTEGER",   mode = "REQUIRED" },
    { name = "event_type",     type = "STRING"                       },
    { name = "event_time",     type = "TIMESTAMP"                    },
    { name = "symbol",         type = "STRING"                       },
    { name = "price",          type = "NUMERIC"                      },
    { name = "quantity",       type = "NUMERIC"                      },
    { name = "trade_time",     type = "TIMESTAMP"                    },
    { name = "is_buyer_maker", type = "BOOLEAN"                      },
    { name = "_insert_date",   type = "TIMESTAMP"                    },
  ])
}

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
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_raw_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_trusted_editor" {
  dataset_id = google_bigquery_dataset.trusted.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
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

# ── BigQuery Scheduled Query: raw → trusted ───────────────────────────────────

# DTS service agent needs to impersonate sa-pipeline to run the scheduled query
resource "google_service_account_iam_member" "dts_token_creator" {
  service_account_id = google_service_account.pipeline.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${var.project_number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}

resource "google_bigquery_data_transfer_config" "raw_to_trusted" {
  display_name           = "raw-to-trusted-btcbrl"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "every 24 hours"
  destination_dataset_id = google_bigquery_dataset.trusted.dataset_id
  service_account_name   = google_service_account.pipeline.email

  params = {
    query = <<-SQL
      MERGE `${var.project_id}.trusted.binance_btc_trades` T
      USING (
        SELECT
          CAST(JSON_VALUE(payload, '$.t') AS INTEGER)                 AS trade_id,
          JSON_VALUE(payload, '$.e')                                  AS event_type,
          TIMESTAMP_MILLIS(CAST(JSON_VALUE(payload, '$.E') AS INT64)) AS event_time,
          JSON_VALUE(payload, '$.s')                                  AS symbol,
          CAST(JSON_VALUE(payload, '$.p') AS NUMERIC)                 AS price,
          CAST(JSON_VALUE(payload, '$.q') AS NUMERIC)                 AS quantity,
          TIMESTAMP_MILLIS(CAST(JSON_VALUE(payload, '$.T') AS INT64)) AS trade_time,
          CAST(JSON_VALUE(payload, '$.m') AS BOOL)                    AS is_buyer_maker,
          CURRENT_TIMESTAMP()                                         AS _insert_date
        FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.t') ORDER BY JSON_VALUE(payload, '$.E') DESC) AS rn
          FROM `${var.project_id}.raw.btcbrl_trades`
          WHERE _load_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        )
        WHERE rn = 1
      ) S ON T.trade_id = S.trade_id
      WHEN NOT MATCHED THEN
        INSERT (trade_id, event_type, event_time, symbol, price, quantity, trade_time, is_buyer_maker, _insert_date)
        VALUES (S.trade_id, S.event_type, S.event_time, S.symbol, S.price, S.quantity, S.trade_time, S.is_buyer_maker, S._insert_date)
    SQL
  }
}
