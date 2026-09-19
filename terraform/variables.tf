variable "subscription_id" {
  description = "Azure subscription to deploy into"
  type        = string
}

variable "prefix" {
  description = "Short name used in every resource name. Change it to deploy a second, parallel environment."
  type        = string
  default     = "medallion"
}

variable "location" {
  description = "Azure region. Everything stays in one region so data transfer between services is free."
  type        = string
  default     = "centralindia"
}

variable "resource_group_name" {
  description = "Resource group to create"
  type        = string
  default     = "rg-medallion-water-tf"
}

variable "sql_admin_login" {
  description = "SQL Server administrator login"
  type        = string
  default     = "rituadmin"
}

variable "sql_admin_password" {
  description = "SQL Server administrator password. Never commit a real value; pass it at run time."
  type        = string
  sensitive   = true
}

variable "client_ip" {
  description = "Public IP allowed through the SQL firewall, e.g. the machine running Terraform"
  type        = string
  default     = "0.0.0.0"
}

variable "databricks_sku" {
  description = "Databricks pricing tier: trial, standard or premium"
  type        = string
  default     = "trial"
}

variable "tags" {
  description = "Tags applied to every resource, which makes cost analysis per project possible"
  type        = map(string)
  default = {
    project     = "medallion-water"
    managed_by  = "terraform"
    environment = "dev"
  }
}
