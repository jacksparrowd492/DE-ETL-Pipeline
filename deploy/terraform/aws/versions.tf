terraform {
  required_version = ">= 1.10" # S3 backend use_lockfile

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.60, < 7.0"
    }
  }

  # State in S3, configured per environment at init time:
  #   terraform init -backend-config=backend-dev.hcl
  # (bucket, key, region, use_lockfile = true). CI's validate step uses -backend=false.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Repository  = "github.com/jacksparrowd492/DE-ETL-Pipeline"
    }
  }
}
