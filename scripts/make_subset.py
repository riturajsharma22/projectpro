"""
Creates the project dataset from the full EEA file.

Input : data/Waterbase_v2025_1_T_WISE6_AggregatedData.csv  (6.45M rows, 2.1 GB)
Output: data/water_quality_2010_2024.csv                   (1.17M rows)

Keeps the 20 most-measured determinands for 2010-2024. All 32 columns and all
data quality problems are kept on purpose; the silver layer cleans them.

Usage:  pip install duckdb
        python scripts/make_subset.py
"""
import duckdb

SRC = "data/Waterbase_v2025_1_T_WISE6_AggregatedData.csv"
OUT = "data/water_quality_2010_2024.csv"

con = duckdb.connect()
con.execute(f"CREATE TABLE t AS SELECT * FROM read_csv('{SRC}', header=true, sample_size=-1)")
con.execute(f"""
COPY (
  SELECT * FROM t
  WHERE phenomenonTimeReferenceYear BETWEEN 2010 AND 2024
    AND observedPropertyDeterminandLabel IN (
      SELECT observedPropertyDeterminandLabel FROM t
      GROUP BY 1 ORDER BY count(*) DESC LIMIT 20)
  ORDER BY UID
) TO '{OUT}' (HEADER, DELIMITER ',', QUOTE '"')
""")
print(con.sql(f"SELECT count(*) FROM read_csv('{OUT}', header=true)").fetchone()[0], "rows written")
