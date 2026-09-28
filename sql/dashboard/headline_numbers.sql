SELECT
    (SELECT COUNT(*) FROM dbw_medallion_water.silver.water_quality)            AS measurements,
    (SELECT COUNT(DISTINCT monitoringSiteIdentifier)
     FROM dbw_medallion_water.silver.water_quality)                            AS monitoring_sites,
    (SELECT COUNT(DISTINCT countryCode) FROM dbw_medallion_water.silver.water_quality) AS countries,
    (SELECT ROUND(AVG(breach_pct), 1) FROM dbw_medallion_water.gold.threshold_breaches
     WHERE determinand = 'Nitrate' AND year >= 2020)                           AS avg_nitrate_breach_pct