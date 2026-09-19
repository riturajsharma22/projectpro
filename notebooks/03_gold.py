# Databricks notebook source
storage_account = "adlsmedallionwater"
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="medallion", key="adls-key")
)

CATALOG = "dbw_medallion_water"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

from pyspark.sql import functions as F

silver = spark.table(f"{CATALOG}.silver.water_quality")
print("Silver rows:", silver.count())

# COMMAND ----------

# Physically possible ranges for the measurements that have a fixed scale
LIMITS = {
    "pH":                      (0, 14),      # the pH scale itself
    "Water temperature":       (-5, 45),     # plausible surface water
    "Dissolved oxygen":        (0, 25),      # mg/L; water saturates near 14
    "Oxygen saturation":       (0, 200),     # %; above 100 happens with algae
    "Electrical conductivity": (0, 100000),  # uS/cm; seawater is about 50,000
}

cond = F.lit(True)
for det, (lo, hi) in LIMITS.items():
    cond = cond & F.when(F.col("determinand") == det,
                         F.col("mean_value").between(lo, hi)).otherwise(F.lit(True))

plausible = silver.withColumn("in_valid_range", cond)

removed = plausible.filter(~F.col("in_valid_range"))
print("Rows outside physical limits:", removed.count())
display(removed.groupBy("determinand")
               .agg(F.count("*").alias("rows"),
                    F.round(F.min("mean_value"), 2).alias("min"),
                    F.round(F.max("mean_value"), 2).alias("max"))
               .orderBy(F.desc("rows")))

gold_base = plausible.filter("in_valid_range").cache()
print("Rows used for gold:", gold_base.count())

# COMMAND ----------

trend = (gold_base.groupBy("countryCode", "determinand", "unit", "year")
    .agg(
        F.count("*").alias("measurement_count"),
        F.countDistinct("monitoringSiteIdentifier").alias("site_count"),
        F.round(F.avg("mean_value"), 4).alias("avg_value"),
        F.round(F.expr("percentile_approx(mean_value, 0.5)"), 4).alias("median_value"),
        F.round(F.expr("percentile_approx(mean_value, 0.95)"), 4).alias("p95_value"),
        F.round(F.max("mean_value"), 4).alias("max_value"),
        F.sum(F.col("has_qc_warning").cast("int")).alias("qc_warning_count"),
    ))

(trend.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.gold.yearly_by_country_determinand"))

print("rows:", trend.count())
display(trend.filter("determinand = 'Nitrate' AND countryCode = 'DE'").orderBy("year"))

# COMMAND ----------

by_body = (gold_base.groupBy("determinand", "unit", "water_body_type", "year")
    .agg(
        F.count("*").alias("measurement_count"),
        F.countDistinct("monitoringSiteIdentifier").alias("site_count"),
        F.round(F.expr("percentile_approx(mean_value, 0.5)"), 4).alias("median_value"),
        F.round(F.avg("mean_value"), 4).alias("avg_value"),
    ))

(by_body.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.gold.by_water_body_type"))

display(by_body.filter("determinand = 'Nitrate' AND year = 2023").orderBy(F.desc("site_count")))

# COMMAND ----------

# EU limits, as a small reference table
THRESHOLDS = [
    ("Nitrate",          "mg{NO3}/L", 50.0,  "EU drinking water limit"),
    ("Nitrite",          "mg{NO2}/L", 0.5,   "EU drinking water limit"),
    ("Lead and its compounds",    "ug/L", 5.0,  "EU drinking water limit"),
    ("Cadmium and its compounds", "ug/L", 5.0,  "EU drinking water limit"),
    ("Arsenic and its compounds", "ug/L", 10.0, "EU drinking water limit"),
    ("Mercury and its compounds", "ug/L", 1.0,  "EU drinking water limit"),
    ("Dissolved oxygen", "mg/L",      5.0,   "Below this stresses fish"),
]

thresholds = spark.createDataFrame(THRESHOLDS, "determinand string, unit string, threshold double, basis string")

# Dissolved oxygen is a breach when it's too LOW; everything else when too HIGH
breach = (gold_base.join(thresholds, ["determinand", "unit"])
    .withColumn("exceeds",
        F.when(F.col("determinand") == "Dissolved oxygen", F.col("mean_value") < F.col("threshold"))
         .otherwise(F.col("mean_value") > F.col("threshold")))
    .groupBy("countryCode", "determinand", "unit", "year", "threshold", "basis")
    .agg(
        F.count("*").alias("measurement_count"),
        F.sum(F.col("exceeds").cast("int")).alias("breach_count"),
        F.round(100 * F.avg(F.col("exceeds").cast("int")), 2).alias("breach_pct"),
    ))

(breach.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.gold.threshold_breaches"))

display(breach.filter("determinand = 'Nitrate' AND year = 2023")
              .orderBy(F.desc("breach_pct")).limit(15))

# COMMAND ----------

quality = spark.sql(f"""
    SELECT 'loaded_from_source'  AS stage, COUNT(*) AS rows FROM {CATALOG}.bronze.water_quality
    UNION ALL SELECT 'passed_to_silver', COUNT(*) FROM {CATALOG}.silver.water_quality
    UNION ALL SELECT 'quarantined',      COUNT(*) FROM {CATALOG}.silver.water_quality_quarantine
    UNION ALL SELECT CONCAT('rejected_', reject_reason), COUNT(*)
              FROM {CATALOG}.silver.water_quality_quarantine GROUP BY reject_reason
""")

(quality.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.gold.pipeline_quality_summary"))

display(quality)
gold_base.unpersist()

# COMMAND ----------

# Add the gold-stage filtering to the quality summary
silver_rows = spark.table(f"{CATALOG}.silver.water_quality").count()
gold_rows   = gold_base.count()          # rows that passed the physical-limit checks
removed_rows = silver_rows - gold_rows

extra = spark.createDataFrame(
    [("removed_outside_physical_limits", removed_rows),
     ("used_in_gold_aggregations", gold_rows)],
    "stage string, rows long")

quality_full = quality.unionByName(extra)

(quality_full.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
             .saveAsTable(f"{CATALOG}.gold.pipeline_quality_summary"))

display(quality_full)

# COMMAND ----------

