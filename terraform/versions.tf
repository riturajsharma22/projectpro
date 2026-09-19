terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# Storage account names must be globally unique, so a short random suffix is
# appended. This lets the same code deploy a second, parallel environment
# without name clashes.
resource "random_string" "suffix" {
  length  = 5
  special = false
  upper   = false
  numeric = true
}
