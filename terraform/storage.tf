# Two storage accounts on purpose:
#   blob  - plain Blob Storage, where the Logic App writes its CSV exports
#   lake  - ADLS Gen2 (hierarchical namespace on), the data lake Databricks reads

resource "azurerm_storage_account" "blob" {
  name                     = "st${var.prefix}blob${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = false # plain Blob Storage
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
}

resource "azurerm_storage_account" "lake" {
  name                     = "adls${var.prefix}${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # this is what makes it ADLS Gen2
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
}

# Containers on the Blob account
resource "azurerm_storage_container" "blob_containers" {
  for_each              = toset(["source-upload", "raw-export"])
  name                  = each.value
  storage_account_id    = azurerm_storage_account.blob.id
  container_access_type = "private"
}

# Medallion zones on the data lake
resource "azurerm_storage_container" "lake_containers" {
  for_each              = toset(["landing", "bronze", "silver", "gold"])
  name                  = each.value
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}
