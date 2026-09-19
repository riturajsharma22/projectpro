# Everything lives in one resource group, so the whole environment can be
# removed with a single terraform destroy.

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}
