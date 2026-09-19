# Databricks notebook source
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

from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, LongType, DoubleType, TimestampType)

schema = StructType([
    StructField("countryCode",                          StringType()),
    StructField("monitoringSiteIdentifier",             StringType()),
    StructField("monitoringSiteIdentifierScheme",       StringType()),
    StructField("parameterWaterBodyCategory",           StringType()),
    StructField("observedPropertyDeterminandCode",      StringType()),
    StructField("observedPropertyDeterminandLabel",     StringType()),
    StructField("procedureAnalysedMatrix",              StringType()),
    StructField("resultUom",                            StringType()),
    StructField("phenomenonTimeReferenceYear",          IntegerType()),
    StructField("parameterSamplingPeriod",              StringType()),
    StructField("procedureLOQValue",                    DoubleType()),
    StructField("resultNumberOfSamples",                IntegerType()),
    StructField("resultQualityNumberOfSamplesBelowLOQ", IntegerType()),
    StructField("resultQualityMinimumBelowLOQ",         IntegerType()),
    StructField("resultMinimumValue",                   DoubleType()),
    StructField("resultQualityMeanBelowLOQ",            IntegerType()),
    StructField("resultMeanValue",                      DoubleType()),
    StructField("resultQualityMaximumBelowLOQ",         IntegerType()),
    StructField("resultMaximumValue",                   DoubleType()),
    StructField("resultQualityMedianBelowLOQ",          IntegerType()),
    StructField("resultMedianValue",                    DoubleType()),
    StructField("resultStandardDeviationValue",         DoubleType()),
    StructField("procedureAnalyticalMethod",            StringType()),
    StructField("parameterSampleDepth",                 DoubleType()),
    StructField("resultObservationStatus",              StringType()),
    StructField("remarks",                              StringType()),
    StructField("metadata_versionId",                   StringType()),
    StructField("metadata_beginLifeSpanVersion",        StringType()),
    StructField("metadata_statusCode",                  StringType()),
    StructField("metadata_observationStatus",           StringType()),
    StructField("metadata_statements",                  StringType()),
    StructField("UID",                                  LongType()),
])
print(f"{len(schema.fields)} data columns defined")

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

bronze.select("metadata_beginLifeSpanVersion", "parameterSamplingPeriod", "resultUom") \
      .filter(F.col("metadata_beginLifeSpanVersion").isNotNull()) \
      .show(5, truncate=False)

# COMMAND ----------

checks = bronze.agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("UID").isNull(), 1).otherwise(0)).alias("null_uid"),
    F.countDistinct("UID").alias("distinct_uid"),
    F.countDistinct("countryCode").alias("countries"),
    F.countDistinct("observedPropertyDeterminandLabel").alias("determinands"),
    F.min("phenomenonTimeReferenceYear").alias("min_year"),
    F.max("phenomenonTimeReferenceYear").alias("max_year"),
    F.countDistinct("_source_file").alias("files"),
    # These confirm the comma-containing text was parsed correctly
    F.sum(F.when(F.col("metadata_statements").contains(","), 1).otherwise(0)).alias("stmt_with_commas"),
    F.sum(F.when(F.col("metadata_beginLifeSpanVersion").isNull(), 1).otherwise(0)).alias("null_lifespan"),
).collect()[0]

for k, v in checks.asDict().items():
    print(f"{k:>18}: {v}")

# COMMAND ----------

(bronze.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.bronze.water_quality"))

print(spark.table(f"{CATALOG}.bronze.water_quality").count(), "rows written")
bronze.unpersist()

# COMMAND ----------

display(spark.sql("""
    SELECT metadata_beginLifeSpanVersion, parameterSamplingPeriod,
           resultUom, observedPropertyDeterminandLabel, resultMeanValue
    FROM dbw_medallion_water.bronze.water_quality
    LIMIT 5
"""))

# COMMAND ----------

