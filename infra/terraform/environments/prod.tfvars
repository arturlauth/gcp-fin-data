project_id      = "gcp-fin-data-prod"
environment     = "prod"
region          = "us-central1"
gcs_bucket_name = "gcp-fin-data-bucket-prod"

producer_image = "us-central1-docker.pkg.dev/gcp-fin-data-prod/cloud-run/producer:latest"
consumer_image = "us-central1-docker.pkg.dev/gcp-fin-data-prod/cloud-run/consumer:latest"
pipeline_image = "us-central1-docker.pkg.dev/gcp-fin-data-prod/cloud-run/pipelines:latest"
