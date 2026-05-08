terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Pub/Sub ───────────────────────────────────────────────────────────────────

resource "google_pubsub_topic" "btcbrl_trades" {
  name = "btcbrl-trades"
}

resource "google_pubsub_subscription" "btcbrl_trades_sub" {
  name  = "btcbrl-trades-sub"
  topic = google_pubsub_topic.btcbrl_trades.id

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"
}

# ── GCS ───────────────────────────────────────────────────────────────────────

resource "google_storage_bucket" "raw" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }
}

# ── BigQuery ──────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "trusted" {
  dataset_id = "trusted"
  location   = "US"
}

resource "google_bigquery_dataset" "refined" {
  dataset_id = "refined"
  location   = "US"
}

resource "google_bigquery_table" "trusted_binance_btc_trades" {
  dataset_id          = google_bigquery_dataset.trusted.dataset_id
  table_id            = "binance_btc_trades"
  deletion_protection = false

  schema = jsonencode([
    { name = "trade_id",       type = "INTEGER",   mode = "REQUIRED" },
    { name = "event_type",     type = "STRING"    },
    { name = "event_time",     type = "TIMESTAMP" },
    { name = "symbol",         type = "STRING"    },
    { name = "price",          type = "NUMERIC"   },
    { name = "quantity",       type = "NUMERIC"   },
    { name = "trade_time",     type = "TIMESTAMP" },
    { name = "is_buyer_maker", type = "BOOLEAN"   },
    { name = "_file_source",   type = "STRING"    },
    { name = "_insert_date",   type = "TIMESTAMP" },
    { name = "_job",           type = "STRING"    },
  ])
}

# ── Artifact Registry ─────────────────────────────────────────────────────────

resource "google_artifact_registry_repository" "cloud_run" {
  location      = var.region
  repository_id = "cloud-run"
  format        = "DOCKER"
}

# ── Service Accounts ──────────────────────────────────────────────────────────

resource "google_service_account" "producer" {
  account_id   = "sa-producer"
  display_name = "Cloud Run Producer"
}

resource "google_service_account" "consumer" {
  account_id   = "sa-consumer"
  display_name = "Cloud Run Consumer"
}

resource "google_service_account" "pipeline" {
  account_id   = "sa-pipeline"
  display_name = "Cloud Run Pipeline Jobs"
}

resource "google_pubsub_topic_iam_member" "producer_publisher" {
  topic  = google_pubsub_topic.btcbrl_trades.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.producer.email}"
}

resource "google_pubsub_subscription_iam_member" "consumer_subscriber" {
  subscription = google_pubsub_subscription.btcbrl_trades_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.consumer.email}"
}

resource "google_storage_bucket_iam_member" "consumer_raw_writer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.consumer.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_raw_reader" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pipeline.email}"
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

# ── Cloud Run ─────────────────────────────────────────────────────────────────

resource "google_cloud_run_v2_service" "producer" {
  name     = "producer"
  location = var.region

  template {
    service_account = google_service_account.producer.email

    containers {
      image = var.producer_image

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "PUBSUB_TOPIC_ID"
        value = google_pubsub_topic.btcbrl_trades.name
      }
    }
  }
}

resource "google_cloud_run_v2_service" "consumer" {
  name     = "consumer"
  location = var.region

  template {
    service_account = google_service_account.consumer.email

    containers {
      image = var.consumer_image

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "PUBSUB_SUBSCRIPTION_ID"
        value = google_pubsub_subscription.btcbrl_trades_sub.name
      }
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.raw.name
      }
      env {
        name  = "GCS_RAW_PREFIX"
        value = "btcbrl/raw"
      }
    }
  }
}

resource "google_cloud_run_v2_job" "btcbrl_raw_trusted" {
  name     = "btcbrl-raw-trusted"
  location = var.region

  template {
    template {
      service_account = google_service_account.pipeline.email

      containers {
        image   = var.pipeline_image
        command = ["python"]
        args    = ["btcbrl_raw_trusted.py"]

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.raw.name
        }
        env {
          name  = "BIGQUERY_DATASET_ID"
          value = google_bigquery_dataset.trusted.dataset_id
        }
        env {
          name  = "BIGQUERY_TABLE_ID"
          value = google_bigquery_table.trusted_binance_btc_trades.table_id
        }
      }
    }
  }
}
