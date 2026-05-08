variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
}

variable "gcs_bucket_name" {
  description = "GCS bucket name for raw trade data"
  type        = string
}

variable "producer_image" {
  description = "Docker image URI for the producer Cloud Run service"
  type        = string
}

variable "consumer_image" {
  description = "Docker image URI for the consumer Cloud Run service"
  type        = string
}

variable "pipeline_image" {
  description = "Docker image URI for the pipelines Cloud Run job"
  type        = string
}
