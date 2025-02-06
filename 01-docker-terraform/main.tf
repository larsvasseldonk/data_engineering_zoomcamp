terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.19.0"
    }
  }
}

provider "google" {
  credentials = ".keys/google_creds.json"
  project = "sandbox-450016"
  region = "west-europe"
}
