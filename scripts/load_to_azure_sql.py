"""
Loads data/water_quality_2010_2024.csv into Azure SQL table dbo.water_quality.

Before running:
  1. Create the table with sql/01_create_table.sql
  2. Install the "ODBC Driver 18 for SQL Server" (Microsoft download)
  3. pip install pandas pyodbc
  4. In PowerShell, set your connection details (these stay out of the code):
       $env:AZSQL_SERVER   = "<your-server>.database.windows.net"
       $env:AZSQL_DB       = "<your-database>"
       $env:AZSQL_USER     = "<your-admin-login>"
       $env:AZSQL_PASSWORD = "<your-password>"
  5. python scripts/load_to_azure_sql.py

The table is emptied first, so the script is safe to re-run.
"""
import os
import time

import pandas as pd
import pyodbc

CSV_PATH = "data/water_quality_2010_2024.csv"
TABLE = "dbo.water_quality"
BATCH_ROWS = 50_000

# Column name -> (kind, max length for text). Order matches the CSV and the table.
COLUMNS = {
    "countryCode": ("text", 5),
    "monitoringSiteIdentifier": ("text", 50),
    "monitoringSiteIdentifierScheme": ("text", 50),
    "parameterWaterBodyCategory": ("text", 5),
    "observedPropertyDeterminandCode": ("text", 30),
    "observedPropertyDeterminandLabel": ("text", 100),
    "procedureAnalysedMatrix": ("text", 10),
    "resultUom": ("text", 30),
    "phenomenonTimeReferenceYear": ("int", None),
    "parameterSamplingPeriod": ("text", 30),
    "procedureLOQValue": ("float", None),
    "resultNumberOfSamples": ("int", None),
    "resultQualityNumberOfSamplesBelowLOQ": ("int", None),
    "resultQualityMinimumBelowLOQ": ("int", None),
    "resultMinimumValue": ("float", None),
    "resultQualityMeanBelowLOQ": ("int", None),
    "resultMeanValue": ("float", None),
    "resultQualityMaximumBelowLOQ": ("int", None),
    "resultMaximumValue": ("float", None),
    "resultQualityMedianBelowLOQ": ("int", None),
    "resultMedianValue": ("float", None),
    "resultStandardDeviationValue": ("float", None),
    "procedureAnalyticalMethod": ("text", 300),
    "parameterSampleDepth": ("float", None),
    "resultObservationStatus": ("text", 5),
    "remarks": ("text", 500),
    "metadata_versionId": ("text", 200),
    "metadata_beginLifeSpanVersion": ("datetime", None),
    "metadata_statusCode": ("text", 20),
    "metadata_observationStatus": ("text", 5),
    "metadata_statements": ("text", 2000),
    "UID": ("bigint", None),
}

PANDAS_DTYPES = {
    "text": "string",
    "int": "Int64",
    "bigint": "Int64",
    "float": "float64",
    "datetime": "string",
}

# Explicit parameter types, so empty values at the start of a batch
# don't make pyodbc guess the wrong type.
INPUT_SIZES = []
for kind, length in COLUMNS.values():
    if kind == "text":
        INPUT_SIZES.append((pyodbc.SQL_WVARCHAR, length, 0))
    elif kind == "int":
        INPUT_SIZES.append((pyodbc.SQL_INTEGER, 0, 0))
    elif kind == "bigint":
        INPUT_SIZES.append((pyodbc.SQL_BIGINT, 0, 0))
    elif kind == "float":
        INPUT_SIZES.append((pyodbc.SQL_DOUBLE, 0, 0))
    elif kind == "datetime":
        INPUT_SIZES.append((pyodbc.SQL_TYPE_TIMESTAMP, 23, 3))


def connect():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{os.environ['AZSQL_SERVER']},1433;"
        f"Database={os.environ['AZSQL_DB']};"
        f"Uid={os.environ['AZSQL_USER']};"
        f"Pwd={os.environ['AZSQL_PASSWORD']};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    # A paused serverless database can take a minute to wake up, so retry.
    for attempt in range(1, 6):
        try:
            return pyodbc.connect(conn_str)
        except pyodbc.Error as err:
            print(f"Connection attempt {attempt} failed: {err}")
            time.sleep(20)
    raise SystemExit("Could not connect. Check the firewall rule and credentials.")


def prepare(chunk: pd.DataFrame) -> list:
    chunk = chunk.copy()
    chunk["metadata_beginLifeSpanVersion"] = pd.to_datetime(
        chunk["metadata_beginLifeSpanVersion"], errors="coerce"
    )
    # Convert pandas missing values (NaN, NA, NaT) to None, which becomes SQL NULL
    chunk = chunk.astype(object).where(chunk.notna(), None)
    rows = []
    for row in chunk.itertuples(index=False, name=None):
        rows.append(
            tuple(v.to_pydatetime() if isinstance(v, pd.Timestamp) else v for v in row)
        )
    return rows


def main():
    conn = connect()
    cursor = conn.cursor()
    cursor.fast_executemany = True

    cursor.execute(f"TRUNCATE TABLE {TABLE}")
    conn.commit()

    placeholders = ", ".join("?" for _ in COLUMNS)
    sql = f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES ({placeholders})"
    dtypes = {name: PANDAS_DTYPES[kind] for name, (kind, _) in COLUMNS.items()}

    loaded = 0
    started = time.time()
    for chunk in pd.read_csv(CSV_PATH, dtype=dtypes, chunksize=BATCH_ROWS):
        cursor.setinputsizes(INPUT_SIZES)
        cursor.executemany(sql, prepare(chunk))
        conn.commit()
        loaded += len(chunk)
        print(f"{loaded:,} rows loaded ({time.time() - started:.0f}s)")

    cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
    print("Rows in table:", cursor.fetchone()[0])
    conn.close()


if __name__ == "__main__":
    main()
