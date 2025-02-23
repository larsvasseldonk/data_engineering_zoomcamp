terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.19.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "mod04-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = false

  # Auto delete bucket after 1 day
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}

# resource "google_bigquery_dataset" "mod04-bq-dataset" {
#   dataset_id                  = var.bq_dataset_name
#   location                    = var.location
# }