variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "project_number" {
  description = "GCP project number (used for DTS service agent IAM binding)"
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
  description = "GCS bucket name for the data lake"
  type        = string
}

variable "binance_streams" {
  description = "Comma-separated Binance stream names, e.g. btcbrl@trade,btcusdt@trade"
  type        = string
  default     = "btcbrl@trade"
}
