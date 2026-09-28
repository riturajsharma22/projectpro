# Terraform: the Azure environment as code

Describes every resource this project runs on: resource group, SQL server and
database, both storage accounts with their six containers, the Logic App, Data
Factory with its linked services, and the Databricks workspace.

```
terraform/
├── versions.tf    provider versions, random suffix for globally unique names
├── variables.tf   inputs (subscription, region, prefix, credentials, client IP)
├── main.tf        resource group
├── sql.tf         SQL server, database, two firewall rules
├── storage.tf     Blob account + ADLS Gen2 lake, 6 containers
├── services.tf    Logic App, Data Factory + linked services, Databricks
├── outputs.tf     names, URLs and the abfss path the notebooks use
└── terraform.tfvars.example   copy to terraform.tfvars and fill in
```

## Status

Written and validated, not applied. The environment it describes was originally
built through the portal; this code reproduces it in a separate resource group
(`rg-medallion-water-tf`), so a plan can be run safely against the same
subscription without touching anything that already exists.

```
terraform validate
  Success! The configuration is valid.

terraform plan
  Plan: 20 to add, 0 to change, 0 to destroy.
```

## Why the resources are defined this way

- **One resource group, one region.** Data transfer between services in the same
  region is free, and the whole environment can be removed in one command.
- **A random 5-character suffix** on globally unique names (storage accounts, SQL
  server). It is also what allows a second, parallel environment: change `prefix`
  and apply again, and nothing clashes.
- **`is_hns_enabled`** is the single setting that separates the two storage
  accounts. `true` makes an account ADLS Gen2, which has real directories and
  atomic renames — what Spark and Delta depend on when committing tables. `false`
  leaves it as plain Blob Storage, which is all the staging account needs. It
  cannot be changed after creation.
- **`for_each` for the containers**, rather than six near-identical blocks.
- **Linked services take their credentials from the storage account resources**,
  so no key is written out. Data Factory also gets a system-assigned identity.
- **Passwords are never in the code.** `sql_admin_password` is marked `sensitive`
  and has no default, so it must be supplied at run time.
- **Tags on everything**, which makes per-project cost analysis possible.

## A provider limitation worth knowing

The Azure SQL free offer (100,000 vCore seconds and 32 GB a month, auto-paused
when the allowance runs out) cannot be set from Terraform with azurerm 4.81:

```
Error: Unsupported argument
  An argument named "free_limit_enabled" is not expected here.
```

Those arguments were added to the provider after that release. They are kept
commented in `sql.tf` so they can be enabled after a provider upgrade, and until
then the offer is applied in the portal. Providers routinely lag behind new Azure
features, and knowing where the tool stops is part of using it.

## Validate (nothing is created, nothing is charged)

Terraform is pre-installed in Azure Cloud Shell, so nothing needs installing
locally.

```bash
cd terraform

export TF_VAR_subscription_id="<your subscription id>"
export TF_VAR_sql_admin_password="<a strong password>"
export TF_VAR_client_ip=$(curl -s ifconfig.me)

terraform init          # downloads the azurerm and random providers
terraform fmt -check    # formatting
terraform validate      # syntax and references, without contacting Azure
terraform plan          # what would happen, without doing any of it
```

Passing the variables as `TF_VAR_*` environment variables keeps credentials out
of both the command history and any file. They can also go in a
`terraform.tfvars` file, which is gitignored.

**Reading the plan.** Each resource is listed with `+ create`. `(known after
apply)` means Azure assigns the value — names carrying the random suffix, for
instance. `(sensitive value)` hides the password and storage keys. The summary
line is what matters: `0 to change, 0 to destroy` confirms nothing existing would
be touched.

## Deploy

```bash
terraform apply     # prints the plan, waits for "yes", ~10 minutes
terraform output    # server name, workspace URL, landing path
```

Most of the ten minutes is the Databricks workspace. The outputs then give the
values the rest of the project needs, including the `abfss://` path the notebooks
read from.

A second, parallel environment:

```bash
terraform apply -var="prefix=medallion2" \
                -var="resource_group_name=rg-medallion-water-tf2"
```

## Destroy

```bash
terraform destroy
```

Removes everything in the resource group. On a trial subscription this is the
real benefit: the environment does not have to exist between working sessions,
because it can be rebuilt exactly, in about ten minutes, whenever it is needed.

**Two things destroy does not remove:** the managed resource group Azure creates
alongside the Databricks workspace (it goes with the workspace), and any data
written into the storage accounts, which goes with the accounts themselves.
Export anything worth keeping first.

## What Terraform does not create

| Not in Terraform | Why |
|---|---|
| Logic App actions (SQL query, Create CSV table, Create blob) | They carry connection references that only exist once the connector has been authorised interactively. Kept as JSON in `logic_app/` and imported after the first apply. |
| Data Factory pipeline and datasets | Same reason. Kept in `adf/`. |
| Databricks cluster | The VM type had to be chosen from what was actually available and had quota at the time; created through the REST API. |
| SQL tables and the `logicapp_reader` user | Schema and permissions belong with the SQL scripts in `sql/`, not with infrastructure. |

