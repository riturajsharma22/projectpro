-- Source table for the Azure Medallion project
-- Dataset: EEA Waterbase - Water Quality ICM (WISE6 AggregatedData), 2026 release
-- Subset: 20 most-measured determinands, years 2010-2024 (1,173,094 rows, 32 columns)
-- Raw data is loaded as-is. Cleaning happens later in the Databricks silver layer.

IF OBJECT_ID('dbo.water_quality', 'U') IS NOT NULL
    DROP TABLE dbo.water_quality;
GO

CREATE TABLE dbo.water_quality (
    countryCode                             VARCHAR(5)      NULL,
    monitoringSiteIdentifier                VARCHAR(50)     NULL,
    monitoringSiteIdentifierScheme          VARCHAR(50)     NULL,
    parameterWaterBodyCategory              VARCHAR(5)      NULL,
    observedPropertyDeterminandCode         VARCHAR(30)     NULL,
    observedPropertyDeterminandLabel        NVARCHAR(100)   NULL,
    procedureAnalysedMatrix                 VARCHAR(10)     NULL,
    resultUom                               NVARCHAR(30)    NULL,
    phenomenonTimeReferenceYear             INT             NULL,
    parameterSamplingPeriod                 VARCHAR(30)     NULL,
    procedureLOQValue                       FLOAT           NULL,
    resultNumberOfSamples                   INT             NULL,
    resultQualityNumberOfSamplesBelowLOQ    INT             NULL,
    resultQualityMinimumBelowLOQ            INT             NULL,
    resultMinimumValue                      FLOAT           NULL,
    resultQualityMeanBelowLOQ               INT             NULL,
    resultMeanValue                         FLOAT           NULL,
    resultQualityMaximumBelowLOQ            INT             NULL,
    resultMaximumValue                      FLOAT           NULL,
    resultQualityMedianBelowLOQ             INT             NULL,
    resultMedianValue                       FLOAT           NULL,
    resultStandardDeviationValue            FLOAT           NULL,
    procedureAnalyticalMethod               NVARCHAR(300)   NULL,
    parameterSampleDepth                    FLOAT           NULL,
    resultObservationStatus                 VARCHAR(5)      NULL,
    remarks                                 NVARCHAR(500)   NULL,
    metadata_versionId                      VARCHAR(200)    NULL,
    metadata_beginLifeSpanVersion           DATETIME2(3)    NULL,
    metadata_statusCode                     VARCHAR(20)     NULL,
    metadata_observationStatus              VARCHAR(5)      NULL,
    metadata_statements                     NVARCHAR(2000)  NULL,
    UID                                     BIGINT          NOT NULL,
    CONSTRAINT PK_water_quality PRIMARY KEY CLUSTERED (UID)
);
GO

-- Run after loading to verify (expected: 1173094 rows, 36 countries, 20 determinands, 2010-2024)
-- SELECT COUNT(*) AS total_rows,
--        COUNT(DISTINCT countryCode) AS countries,
--        COUNT(DISTINCT observedPropertyDeterminandLabel) AS determinands,
--        MIN(phenomenonTimeReferenceYear) AS min_year,
--        MAX(phenomenonTimeReferenceYear) AS max_year
-- FROM dbo.water_quality;
