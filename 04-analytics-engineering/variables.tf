variable "credentials" {
  description = "Credentials"
  default = "/home/lars/.gc/gcs.json"
}

variable "project" {
    description = "Project"
    default = "sandbox-450016"
}

variable "region" {
    description = "Region"
    default = "west-europe"
}

variable "location" {
    description = "Project Location"
    default = "EUROPE-WEST4"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default = "mod04_bq-dataset"
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Name"
  default = "mod04-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}