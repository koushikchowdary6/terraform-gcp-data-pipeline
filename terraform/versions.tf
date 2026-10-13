terraform {
  # Pinned to a major version so a provider release cannot change plan output
  # without an explicit, reviewed bump.
  required_version = ">= 1.5.0, < 2.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.8"
    }
  }

  # Remote state is intentionally NOT configured here.
  #
  # State contains every resource attribute, including generated names and
  # IAM bindings, so it must never be committed (see ../.gitignore). For a
  # single operator, local state plus a backed-up directory is adequate. For
  # anything shared, uncomment the block below and create the bucket first -
  # by hand or in a separate bootstrap configuration, because a backend
  # cannot depend on a resource in the same configuration that uses it.
  #
  # backend "gcs" {
  #   bucket = "my-tfstate-bucket"
  #   prefix = "terraform-gcp-data-pipeline"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
