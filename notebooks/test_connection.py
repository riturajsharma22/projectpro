# Databricks notebook source
# Give Spark the storage key from the secret scope
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

# COMMAND ----------

