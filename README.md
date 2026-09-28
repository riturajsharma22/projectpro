# Azure Medallion Architecture Data Pipeline

An end-to-end Azure data pipeline that moves 1.17 million European water quality
measurements from a SQL database into a data lake, refines them through bronze,
silver and gold layers in Databricks, and reports on them in a dashboard. The
whole Azure environment is also described in Terraform.

**Source data:** European Environment Agency,
[Waterbase – Water Quality ICM](https://www.eea.europa.eu/en/datahub) (2026
release). 20 most-measured substances, 2010–2024, 36 countries.

---

## Architecture

```
Azure SQL Database            1,173,094 rows · 32 columns          source system
        │
        │  Logic App          batches of 20,000 rows, 3.1 minutes
        ▼
Blob Storage                  59 CSV files · 414 MB                raw-export
        │
        │  Azure Data Factory binary copy · 59 files · 22 seconds
        ▼
ADLS Gen2                     landing/water_quality/               data lake
        │
        │  Databricks (PySpark on Delta)
        ├─ Bronze             raw, typed, nothing cleaned          1,173,094
        ├─ Silver             cleaned + quarantine table           1,158,463
        └─ Gold               4 aggregated tables                  1,158,006
                                      │
                                      ▼
                            Databricks SQL dashboard
```

Everything runs in one region (Central India) and one resource group, so data
transfer between services is free and the environment can be removed in one step.

---

## Row counts at every stage

The same figure is checked after each hop, which is how the pipeline proves
nothing was lost or duplicated in transit.

| Stage | Rows | Note |
|---|---:|---|
| Azure SQL source | 1,173,094 | Loaded from the EEA CSV |
| Bronze | 1,173,094 | Unchanged across five systems |
| Silver | 1,158,463 | Usable rows |
| Quarantine | 14,631 | Kept, with a reason on every row |
| Gold | 1,158,006 | 457 physically impossible values removed |

`1,158,463 + 14,631 = 1,173,094`, and the quarantine reasons
(13,879 missing mean + 399 invalid sample count + 353 minimum greater than
maximum) add back to 14,631 exactly.

---

## What the data shows

- **Groundwater is the worst affected** by nitrate: a median around 15 mg/L
  against roughly 2 mg/L in lakes. Fertiliser nitrate seeps into groundwater and
  stays there for years.
- **Malta breached the EU nitrate limit (50 mg/L) in 77.5%** of its 2023
  measurements; Spain in 12.7% of 7,393.
- Trends are reported as **medians**, because a handful of extreme sites would
  otherwise dominate a country's line.

---

## Repository layout

```
.
├── data/                      not committed (398 MB CSV)
├── scripts/
│   ├── make_subset.py         6.45M source rows -> 1.17M project subset
│   └── load_to_azure_sql.py   alternative loader (pyodbc, fast_executemany)
├── sql/
│   ├── 01_create_table.sql    source table, 32 columns, UID primary key
│   ├── 02_load_from_blob.sql  BULK INSERT from Blob Storage via SAS
│   ├── 03_create_logicapp_user.sql   read-only login for the Logic App
│   └── dashboard/             the SQL behind each dashboard tile
├── logic_app/workflow.json    export workflow (trigger, loop, SQL, blob)
├── adf/                       pipeline and dataset definitions
├── notebooks/
│   ├── test_connection.py
│   ├── 01_bronze.py
│   ├── 02_silver.py
│   └── 03_gold.py
├── terraform/                 the environment as code (see terraform/README.md)
└── docs/
    ├── data_profile.md              columns and known data problems
    └── dashboard.pdf                exported dashboard
```

---

## How it was built

### 1. Source data
The EEA file holds 6,450,192 rows across 32 columns (2.1 GB), which is heavier
than a free-tier database needs to carry. `scripts/make_subset.py` keeps the 20
most frequently measured substances for 2010–2024, giving **1,173,094 rows** that
still cover every country and support 15-year trends. All 32 columns and all data
quality problems are kept deliberately: cleaning belongs in the silver layer.

### 2. Azure SQL
`sql/01_create_table.sql` creates the source table with types derived from
profiling the real file, and `UID` as the primary key. The CSV is uploaded once to
Blob Storage and loaded with `BULK INSERT` through a short-lived SAS credential,
which is dropped afterwards, rather than pushing 400 MB from a laptop.

### 3. Logic App → Blob
A Logic App passes data between actions as JSON, roughly three times the size of
CSV, so a single query for 1.17M rows would never complete. The workflow loops
over batch numbers and pages through the table with `OFFSET`/`FETCH NEXT`,
writing **59 CSV files**, four batches at a time. The app is disabled after the
export, because each trigger would re-export everything and consume the
database's monthly free compute allowance.

### 4. Data Factory → data lake
A Copy activity with **Binary** datasets moves the files byte for byte into
`landing/water_quality/`: 59 files, 413.703 MB read and written, 22 seconds.
Binary mode means Data Factory never parses the CSV, so nothing can be altered.

### 5. Databricks medallion layers
- **Bronze** reads all 59 files with an explicit 32-column schema and writes them
  to Delta unchanged, adding only the source file name and load timestamp.
- **Silver** derives readable columns, standardises units, and splits the data
  into usable rows and a quarantine table that records why each row was excluded.
- **Gold** removes physically impossible values and builds four summary tables so
  the dashboard never scans a million rows.

The storage key lives in a **Databricks secret scope**, so it never appears in a
notebook, an export or this repository.

### 6. Dashboard
Built in Databricks SQL on the gold tables: headline counters, nitrate breaches
by country, median trends by country, a water body comparison, and a data quality
panel driven by `gold.pipeline_quality_summary`.

### 7. Terraform
`terraform/` describes the whole environment: resource group, SQL server and
database, both storage accounts with their six containers, Logic App, Data
Factory with linked services, and the Databricks workspace.
See [terraform/README.md](terraform/README.md) for the commands.

```
terraform init && terraform validate && terraform plan
Plan: 20 to add, 0 to change, 0 to destroy.
```

---

## Design decisions

**Two storage accounts, not one.** A plain Blob account receives the Logic App
export; a second account with `is_hns_enabled = true` is the ADLS Gen2 lake.
The hierarchical namespace gives real directories, which analytics engines
expect, and it cannot be switched on after creation.

**Quality checks in two stages.** Silver removes structurally broken rows
(missing mean, minimum greater than maximum, invalid sample counts). Gold removes
physically impossible values for measurements with a known scale — a pH of 1515,
a water temperature of 1729 °C — while leaving chemical concentrations untouched,
because genuine pollution can be extreme.

**Quarantine, never delete.** Rejected rows keep their reason in a separate
table, so the pipeline can always explain itself and the counts reconcile.

**Units standardised before aggregation.** Two of three cases were naming
differences (`mg{O2}/L` is the same as `mg/L`). Phosphate was a genuine chemical
conversion: `mg{PO4}/L` measures the whole phosphate molecule, `mg{P}/L` only the
phosphorus in it, so 930 rows are multiplied by 30.97 / 94.97 ≈ 0.3261. Without
it those sites would read about three times more polluted than they are.

**Least privilege.** The Logic App connects as `logicapp_reader`, a contained
database user with `db_datareader` only, never as the server admin.

**Bronze stays faithful.** Timestamps are read as text in bronze and converted in
silver, so bronze remains an exact copy of the source.

**Silver partitioned by year**, so year-filtered queries read only the files they
need.

---

## Problems hit, and how they were handled

| Problem | Resolution |
|---|---|
| Browser SQL editor timed out mid-load at 599,999 rows | Re-ran `BULK INSERT` with `FIRSTROW`/`LASTROW` windows; the primary key on `UID` made duplicate rows impossible |
| Logic App could not return 1.17M rows in one query | Paged with `OFFSET`/`FETCH NEXT` into 59 batches of 20,000 |
| Databricks cluster failed with *Cloud Provider Resource Stockout*, and most VM families showed a quota of 0 | Queried `az vm list-skus` and `az vm list-usage`, cross-referenced with the Databricks node-type API, and created the cluster on `Standard_D4ds_v6` via the REST API |
| Every row flagged as corrupt in the first bronze read | The schema had 33 columns against the file's 32; removing the extra `_corrupt_record` field fixed it |
| All timestamps parsed as null | Source uses ISO format (`2022-01-13T08:27:41`); the column is now read as text in bronze and converted in silver |
| Terraform rejected the SQL free-offer arguments | Not present in azurerm 4.81; kept commented in `sql.tf` with the offer applied in the portal |

---

## Running it yourself

1. Download `WISE6_AggregatedData-csv.zip` from the EEA Datahub and unzip it into
   `data/`.
2. `pip install duckdb && python scripts/make_subset.py`
3. Create the Azure resources, either through the portal or with
   `terraform apply` in `terraform/`.
4. Run `sql/01_create_table.sql`, then `02_load_from_blob.sql` after uploading
   the subset to the `source-upload` container.
5. Run `sql/03_create_logicapp_user.sql`, import `logic_app/workflow.json`,
   authorise the SQL and Blob connections, and run it once.
6. Import `adf/` into Data Factory and run the copy pipeline.
7. Create the Databricks secret scope, then run the notebooks in order:
   `00`, `01`, `02`, `03`.
8. Build the dashboard from `sql/dashboard/`.

**Cost note.** On a trial subscription the pipeline runs inside the free
allowances apart from Databricks compute. Terminate the cluster when not in use
and run `terraform destroy` between sessions.
