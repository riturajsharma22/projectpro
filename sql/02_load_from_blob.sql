-- Loads water_quality_2010_2024.csv from Azure Blob Storage into dbo.water_quality
-- Run 01_create_table.sql first.
--
-- Replace the three placeholders before running:
--   <STRONG_PASSWORD>     any strong password (protects the stored SAS token)
--   <SAS_TOKEN>           container SAS token, starting with "sp=" or "sv=" (remove a leading "?")
--   <STORAGE_ACCOUNT>     your storage account name
--   (container name below is "source-upload"; change it if yours differs)

-- 1. Master key (needed once per database to store credentials)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<STRONG_PASSWORD>';
GO

-- 2. Credential holding the SAS token
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'SourceUploadBlob')
    DROP EXTERNAL DATA SOURCE SourceUploadBlob;
IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'SourceUploadCred')
    DROP DATABASE SCOPED CREDENTIAL SourceUploadCred;
GO

CREATE DATABASE SCOPED CREDENTIAL SourceUploadCred
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
     SECRET = '<SAS_TOKEN>';
GO

-- 3. Pointer to the Blob container
CREATE EXTERNAL DATA SOURCE SourceUploadBlob
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://<STORAGE_ACCOUNT>.blob.core.windows.net/source-upload',
    CREDENTIAL = SourceUploadCred
);
GO

-- 4. Load the file (empty table first so the script can be re-run)
TRUNCATE TABLE dbo.water_quality;

BULK INSERT dbo.water_quality
FROM 'water_quality_2010_2024.csv'
WITH (
    DATA_SOURCE = 'SourceUploadBlob',
    FORMAT = 'CSV',
    FIRSTROW = 2,            -- skip the header row
    FIELDQUOTE = '"',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',  -- the file uses LF line endings
    CODEPAGE = '65001',      -- UTF-8
    TABLOCK,
    BATCHSIZE = 100000
);
GO

-- 5. Verify (expected: 1173094 rows, 36 countries, 20 determinands, 2010-2024)
SELECT COUNT(*)                                  AS total_rows,
       COUNT(DISTINCT countryCode)               AS countries,
       COUNT(DISTINCT observedPropertyDeterminandLabel) AS determinands,
       MIN(phenomenonTimeReferenceYear)          AS min_year,
       MAX(phenomenonTimeReferenceYear)          AS max_year
FROM dbo.water_quality;
GO

-- 6. Optional cleanup once the load is verified (removes the stored SAS token)
-- DROP EXTERNAL DATA SOURCE SourceUploadBlob;
-- DROP DATABASE SCOPED CREDENTIAL SourceUploadCred;
