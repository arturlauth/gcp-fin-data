output "pubsub_topic" {
  value = google_pubsub_topic.btcbrl_trades.name
}

output "pubsub_subscription" {
  value = google_pubsub_subscription.btcbrl_trades_sub.name
}

output "gcs_bucket" {
  value = google_storage_bucket.raw.name
}

output "artifact_registry_repo" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.cloud_run.repository_id}"
}

output "producer_sa" {
  value = google_service_account.producer.email
}

output "consumer_sa" {
  value = google_service_account.consumer.email
}

output "pipeline_sa" {
  value = google_service_account.pipeline.email
}
