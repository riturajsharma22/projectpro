# Databricks notebook source
storage_account = "adlsmedallionwater"
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="medallion", key="adls-key")
)

CATALOG = "dbw_medallion_water"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

from pyspark.sql import functions as F

bronze = spark.table(f"{CATALOG}.bronze.water_quality")
print("Bronze rows:", bronze.count())

# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("observedPropertyDeterminandLabel")
multi = (bronze.groupBy("observedPropertyDeterminandLabel", "resultUom")
               .count()
               .withColumn("units_for_determinand", F.count("*").over(w))
               .filter("units_for_determinand > 1")
               .orderBy("observedPropertyDeterminandLabel", F.desc("count")))
display(multi)

# COMMAND ----------

# The dominant unit per substance becomes the standard one
main_unit = (bronze.groupBy("observedPropertyDeterminandLabel", "resultUom")
                   .count()
                   .withColumn("rank", F.row_number().over(
                       Window.partitionBy("observedPropertyDeterminandLabel").orderBy(F.desc("count"))))
                   .filter("rank = 1")
                   .select("observedPropertyDeterminandLabel",
                           F.col("resultUom").alias("standard_uom")))

flagged = (bronze.join(main_unit, "observedPropertyDeterminandLabel", "left")
    # Convert the ISO timestamp text into a real timestamp
    .withColumn("lifespan_ts", F.to_timestamp("metadata_beginLifeSpanVersion", "yyyy-MM-dd'T'HH:mm:ss"))
    # Pull the start year-month out of either sampling-period format
    .withColumn("period_start", F.regexp_extract("parameterSamplingPeriod", r"^(\d{4}-\d{2})", 1))
    # A readable name for the water body type
    .withColumn("water_body_type", F.when(F.col("parameterWaterBodyCategory") == "RW", "River")
                                    .when(F.col("parameterWaterBodyCategory") == "LW", "Lake")
                                    .when(F.col("parameterWaterBodyCategory") == "GW", "Groundwater")
                                    .when(F.col("parameterWaterBodyCategory") == "TW", "Transitional")
                                    .when(F.col("parameterWaterBodyCategory") == "CW", "Coastal")
                                    .otherwise("Unknown"))
    # Mark rows carrying an EEA quality-check message
    .withColumn("has_qc_warning", F.col("metadata_statements").isNotNull())
    # Why a row is unusable, if it is
    .withColumn("reject_reason",
        F.when(F.col("resultMeanValue").isNull(), "missing_mean")
         .when(F.col("resultMinimumValue") > F.col("resultMaximumValue"), "min_gt_max")
         .when(F.col("resultNumberOfSamples").isNull() | (F.col("resultNumberOfSamples") <= 0), "invalid_sample_count")
         .when((F.col("resultMeanValue") < 0) &
               (F.col("observedPropertyDeterminandLabel") != "Water temperature"), "negative_value")
         .otherwise(None)))

flagged.cache()
display(flagged.groupBy("reject_reason").count().orderBy(F.desc("count")))

# COMMAND ----------

P_FACTOR = 30.973762 / 94.971362   # PO4 -> P, about 0.3261

value_cols = ["resultMinimumValue", "resultMeanValue", "resultMaximumValue",
              "resultMedianValue", "resultStandardDeviationValue", "procedureLOQValue"]

needs_conversion = (F.col("observedPropertyDeterminandLabel") == "Phosphate") & \
                   (F.col("resultUom") == "mg{PO4}/L")

converted = flagged
for c in value_cols:
    converted = converted.withColumn(
        c, F.when(needs_conversion, F.col(c) * F.lit(P_FACTOR)).otherwise(F.col(c)))

converted = (converted
    .withColumn("unit_converted", needs_conversion)
    # One consistent unit label per substance
    .withColumn("uom_clean",
        F.when(F.col("resultUom") == "mg{O2}/L", "mg/L")
         .when(F.col("resultUom") == "%{oxygenSaturation}", "%")
         .when(F.col("resultUom") == "mg{PO4}/L", "mg{P}/L")
         .otherwise(F.col("resultUom"))))

# Check: each substance should now have exactly one unit
display(converted.groupBy("observedPropertyDeterminandLabel")
                 .agg(F.countDistinct("uom_clean").alias("units"),
                      F.collect_set("uom_clean").alias("unit_list"))
                 .orderBy(F.desc("units")))

# COMMAND ----------

silver = (converted.filter(F.col("reject_reason").isNull())
    .select(
        "UID", "countryCode", "monitoringSiteIdentifier", "monitoringSiteIdentifierScheme",
        F.col("parameterWaterBodyCategory").alias("water_body_code"),
        "water_body_type",
        F.col("observedPropertyDeterminandLabel").alias("determinand"),
        F.col("observedPropertyDeterminandCode").alias("determinand_code"),
        F.col("uom_clean").alias("unit"),
        F.col("phenomenonTimeReferenceYear").alias("year"),
        "period_start",
        F.col("resultNumberOfSamples").alias("sample_count"),
        F.col("resultMinimumValue").alias("min_value"),
        F.col("resultMeanValue").alias("mean_value"),
        F.col("resultMaximumValue").alias("max_value"),
        F.col("resultMedianValue").alias("median_value"),
        F.col("resultStandardDeviationValue").alias("stddev_value"),
        F.col("procedureLOQValue").alias("loq_value"),
        F.col("procedureAnalysedMatrix").alias("matrix"),
        F.col("resultObservationStatus").alias("observation_status"),
        F.col("metadata_statusCode").alias("status_code"),
        "has_qc_warning", "unit_converted",
        F.col("lifespan_ts").alias("record_updated_at"),
        "_source_file", "_ingested_at",
    ))

quarantine = converted.filter(F.col("reject_reason").isNotNull()) \
                      .select("UID", "countryCode", "observedPropertyDeterminandLabel",
                              "phenomenonTimeReferenceYear", "resultMinimumValue",
                              "resultMeanValue", "resultMaximumValue",
                              "resultNumberOfSamples", "reject_reason",
                              "_source_file", "_ingested_at")

(silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .partitionBy("year")
       .saveAsTable(f"{CATALOG}.silver.water_quality"))

(quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .saveAsTable(f"{CATALOG}.silver.water_quality_quarantine"))

print("silver:    ", spark.table(f"{CATALOG}.silver.water_quality").count())
print("quarantine:", spark.table(f"{CATALOG}.silver.water_quality_quarantine").count())
print("total:     ", spark.table(f"{CATALOG}.silver.water_quality").count()
                   + spark.table(f"{CATALOG}.silver.water_quality_quarantine").count())

# COMMAND ----------

display(spark.sql(f"""
    SELECT determinand, unit, COUNT(*) AS rows,
           ROUND(MIN(mean_value), 4) AS min_mean,
           ROUND(AVG(mean_value), 4) AS avg_mean,
           ROUND(MAX(mean_value), 2) AS max_mean
    FROM {CATALOG}.silver.water_quality
    GROUP BY determinand, unit
    ORDER BY rows DESC
"""))

# COMMAND ----------

