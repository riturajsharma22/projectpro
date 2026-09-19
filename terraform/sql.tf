# Source system: the water quality measurements are loaded here first, and the
# Logic App reads from this database.

resource "azurerm_mssql_server" "main" {
  name                         = "sql-${var.prefix}-${random_string.suffix.result}"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_login
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"
  tags                         = var.tags
}

# Serverless General Purpose, which is what the free offer runs on.
resource "azurerm_mssql_database" "main" {
  name           = "sqldb-water-quality"
  server_id      = azurerm_mssql_server.main.id
  sku_name       = "GP_S_Gen5_2"
  max_size_gb    = 32
  zone_redundant = false

  # The free offer (100,000 vCore seconds + 32 GB a month, auto-paused when the
  # allowance runs out) cannot be set from Terraform with azurerm 4.81: the
  # arguments below were added to the provider after this version. They are kept
  # here, commented, so they can be enabled once the provider is upgraded.
  # Until then the offer is applied in the portal, which is a normal situation
  # with Terraform: providers lag behind new Azure features.
  #
  # free_limit_enabled             = true
  # free_limit_exhaustion_behavior = "AutoPause"

  # Locally redundant backups: geo-redundant storage is far more expensive and
  # unnecessary for a project database.
  storage_account_type = "Local"

  tags = var.tags
}

# Lets Azure services (the Logic App, Data Factory) reach the server.
resource "azurerm_mssql_firewall_rule" "azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# Lets the machine running Terraform (and the Query editor) connect.
resource "azurerm_mssql_firewall_rule" "client" {
  name             = "ClientIP"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = var.client_ip
  end_ip_address   = var.client_ip
}
