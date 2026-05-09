output "gcs_bucket" {
  value = google_storage_bucket.landing.name
}

output "cloud_function_url" {
  value = google_cloudfunctions2_function.landing_to_raw.service_config[0].uri
}

output "artifact_registry_repo" {
  value = "${google_artifact_registry_repository.cloud_run.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.cloud_run.repository_id}"
}

output "streamer_external_ip" {
  value = google_compute_instance.streamer.network_interface[0].access_config[0].nat_ip
}

output "streamer_sa" {
  value = google_service_account.streamer.email
}

output "pipeline_sa" {
  value = google_service_account.pipeline.email
}
