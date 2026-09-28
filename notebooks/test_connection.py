# Databricks notebook source
# MAGIC %md
# MAGIC # Test connection to ADLS Gen2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 - Confirm Databricks can read the data lake
# MAGIC
# MAGIC Run once before the bronze notebook
# MAGIC It proves the secret scope works and the 59 files are visible.

# COMMAND ----------

storage_account = "adlsmedallionwater"
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="medallion", key="adls-key")
)

landing_path = f"abfss://landing@{storage_account}.dfs.core.windows.net/water_quality/"

files = dbutils.fs.ls(landing_path)
print("Files found:", len(files))
for f in files[:3]:
    print(f.name, round(f.size / 1024 / 1024, 2), "MB")