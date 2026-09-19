output "resource_group" {
  description = "Resource group holding every resource"
  value       = azurerm_resource_group.main.name
}

output "sql_server_fqdn" {
  description = "Server name to use in connection strings and in the Logic App connection"
  value       = azurerm_mssql_server.main.fully_qualified_domain_name
}

output "sql_database" {
  value = azurerm_mssql_database.main.name
}

output "blob_account" {
  description = "Plain Blob Storage account: source-upload and raw-export containers"
  value       = azurerm_storage_account.blob.name
}

output "lake_account" {
  description = "ADLS Gen2 data lake: landing, bronze, silver and gold containers"
  value       = azurerm_storage_account.lake.name
}

output "lake_landing_path" {
  description = "Path the Databricks notebooks read from"
  value       = "abfss://landing@${azurerm_storage_account.lake.name}.dfs.core.windows.net/water_quality/"
}

output "data_factory" {
  value = azurerm_data_factory.main.name
}

output "logic_app" {
  value = azurerm_logic_app_workflow.sql_to_blob.name
}

output "databricks_workspace_url" {
  description = "Open this to reach the workspace"
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}
