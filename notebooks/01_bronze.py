# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze layer - raw ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 - Configuration
# MAGIC
# MAGIC Gives Spark the storage account key so it can read the data lake
# MAGIC The key is fetched from the Databricks secret scope, so it never appears in the notebook, in exported files, or on GitHub
# MAGIC The schema (a group of tables) for the bronze layer is created if it does not already exist.

# COMMAND ----------

# Storage access via the secret scope
storage_account = "adlsmedallionwater"
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="medallion", key="adls-key")
)

CATALOG = "dbw_medallion_water"
LANDING = f"abfss://landing@{storage_account}.dfs.core.windows.net/water_quality/"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"USE CATALOG {CATALOG}")
print("Ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 - Column definitions
# MAGIC
# MAGIC Declares the 32 columns and their types explicitly instead of letting Spark guess
# MAGIC This makes the read faster and the result predictable
# MAGIC metadata_beginLifeSpanVersion is deliberately read as text: the source writes ISO timestamps (2022-01-13T08:27:41) and converting them belongs in the silver layer, not in bronze.

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, LongType, DoubleType)

schema = StructType([
    StructField("countryCode", StringType()),
    StructField("monitoringSiteIdentifier", StringType()),
    StructField("monitoringSiteIdentifierScheme", StringType()),
    StructField("parameterWaterBodyCategory", StringType()),
    StructField("observedPropertyDeterminandCode", StringType()),
    StructField("observedPropertyDeterminandLabel", StringType()),
    StructField("procedureAnalysedMatrix", StringType()),
    StructField("resultUom", StringType()),
    StructField("phenomenonTimeReferenceYear", IntegerType()),
    StructField("parameterSamplingPeriod", StringType()),
    StructField("procedureLOQValue", DoubleType()),
    StructField("resultNumberOfSamples", IntegerType()),
    StructField("resultQualityNumberOfSamplesBelowLOQ", IntegerType()),
    StructField("resultQualityMinimumBelowLOQ", IntegerType()),
    StructField("resultMinimumValue", DoubleType()),
    StructField("resultQualityMeanBelowLOQ", IntegerType()),
    StructField("resultMeanValue", DoubleType()),
    StructField("resultQualityMaximumBelowLOQ", IntegerType()),
    StructField("resultMaximumValue", DoubleType()),
    StructField("resultQualityMedianBelowLOQ", IntegerType()),
    StructField("resultMedianValue", DoubleType()),
    StructField("resultStandardDeviationValue", DoubleType()),
    StructField("procedureAnalyticalMethod", StringType()),
    StructField("parameterSampleDepth", DoubleType()),
    StructField("resultObservationStatus", StringType()),
    StructField("remarks", StringType()),
    StructField("metadata_versionId", StringType()),
    StructField("metadata_beginLifeSpanVersion", StringType()),
    StructField("metadata_statusCode", StringType()),
    StructField("metadata_observationStatus", StringType()),
    StructField("metadata_statements", StringType()),
    StructField("UID", LongType()),
])
print(f"{len(schema.fields)} data columns defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 - Read the 59 CSV files
# MAGIC
# MAGIC Reads every file in the landing folder in one command
# MAGIC quote and escape handle the 198,714 rows whose quality-check text contains commas
# MAGIC Two tracking columns are added: which file each row came from, and when it was loaded
# MAGIC cache() keeps the result in memory so the checks in Cell 4 do not re-read the files.

# COMMAND ----------

from pyspark.sql import functions as F

raw = (spark.read
       .option("header", True)
       .option("quote", '"')
       .option("escape", '"')
       .option("multiLine", False)
       .schema(schema)
       .csv(LANDING))

bronze = (raw
          .withColumn("_source_file", F.col("_metadata.file_name"))
          .withColumn("_ingested_at", F.current_timestamp()))

bronze.cache()
print("Total rows:", bronze.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 - Verify nothing was lost in transit
# MAGIC
# MAGIC The data has travelled through five systems (Azure SQL, Logic App, Blob Storage, Data Factory, the data lake)
# MAGIC These checks prove it arrived intact
# MAGIC UID is the last column in every row, so if commas had broken the parsing the values would have shifted and UID would be null or duplicated.

# COMMAND ----------

checks = bronze.agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("UID").isNull(), 1)
           .otherwise(0)).alias("null_uid"),
    F.countDistinct("UID").alias("distinct_uid"),
    F.countDistinct("countryCode").alias("countries"),
    F.countDistinct("observedPropertyDeterminandLabel")
     .alias("determinands"),
    F.min("phenomenonTimeReferenceYear").alias("min_year"),
    F.max("phenomenonTimeReferenceYear").alias("max_year"),
    F.countDistinct("_source_file").alias("files"),
    F.sum(F.when(F.col("metadata_statements").contains(","), 1)
           .otherwise(0)).alias("stmt_with_commas"),
    F.sum(F.when(F.col("metadata_beginLifeSpanVersion").isNull(), 1)
           .otherwise(0)).alias("null_lifespan"),
).collect()[0]

for k, v in checks.asDict().items():
    print(f"{k:>18}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 - Write the bronze Delta table
# MAGIC
# MAGIC Delta is the table format Databricks uses: the data is stored as files, with transactions, versioning and SQL access added on top
# MAGIC Bronze holds the data exactly as it arrived, with no cleaning, so the original is always available to go back to.

# COMMAND ----------

(bronze.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.bronze.water_quality"))

print(spark.table(f"{CATALOG}.bronze.water_quality").count(), "rows written")
bronze.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 - Quick look at the data
# MAGIC
# MAGIC A visual check of what actually landed
# MAGIC This is also where the timestamp format and the unit codes were confirmed.

# COMMAND ----------

display(spark.sql("""
    SELECT metadata_beginLifeSpanVersion, parameterSamplingPeriod,
           resultUom, observedPropertyDeterminandLabel, resultMeanValue
    FROM dbw_medallion_water.bronze.water_quality
    LIMIT 5
"""))