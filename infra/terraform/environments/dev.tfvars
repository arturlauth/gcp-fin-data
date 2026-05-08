project_id      = "gcp-fin-data"
environment     = "dev"
region          = "us-central1"
gcs_bucket_name = "gcp-fin-data-bucket-dev"

producer_image = "us-central1-docker.pkg.dev/gcp-fin-data/cloud-run/producer:latest"
consumer_image = "us-central1-docker.pkg.dev/gcp-fin-data/cloud-run/consumer:latest"
pipeline_image = "us-central1-docker.pkg.dev/gcp-fin-data/cloud-run/pipelines:latest"
