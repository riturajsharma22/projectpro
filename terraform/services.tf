# Logic App: exports the SQL table to Blob Storage in batches.
# Terraform creates the workflow and its weekly trigger. The SQL query and blob
# actions are kept in logic_app/workflow.json and imported into the workflow,
# because they carry connection references that are created when the connector
# is first authorised.
resource "azurerm_logic_app_workflow" "sql_to_blob" {
  name                = "logic-sql-to-blob-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = var.tags
}

# Weekly rather than frequent: every trigger would re-export the whole table and
# consume the database's monthly free compute allowance.
resource "azurerm_logic_app_trigger_recurrence" "weekly" {
  name         = "Recurrence"
  logic_app_id = azurerm_logic_app_workflow.sql_to_blob.id
  frequency    = "Week"
  interval     = 1
}

# Data Factory: copies the exported CSV files from Blob Storage into the lake.
resource "azurerm_data_factory" "main" {
  name                = "adf-${var.prefix}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }
}

# Linked services: how Data Factory reaches each storage account.
resource "azurerm_data_factory_linked_service_azure_blob_storage" "blob" {
  name              = "ls_blob_raw"
  data_factory_id   = azurerm_data_factory.main.id
  connection_string = azurerm_storage_account.blob.primary_connection_string
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "lake" {
  name                = "ls_adls"
  data_factory_id     = azurerm_data_factory.main.id
  url                 = azurerm_storage_account.lake.primary_dfs_endpoint
  storage_account_key = azurerm_storage_account.lake.primary_access_key
}

# Databricks: runs the bronze, silver and gold notebooks.
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${var.prefix}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.databricks_sku
  tags                = var.tags
}
