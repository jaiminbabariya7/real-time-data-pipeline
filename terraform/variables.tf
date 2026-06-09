variable "project_id" {
  description = "GCP project ID"
  type        = string
}
variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}
variable "environment" {
  description = "dev | staging | prod"
  type        = string
  default     = "dev"
}
