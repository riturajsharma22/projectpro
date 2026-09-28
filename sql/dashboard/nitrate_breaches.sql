SELECT countryCode AS country, year, threshold,
       measurement_count, breach_count, breach_pct
FROM dbw_medallion_water.gold.threshold_breaches
WHERE determinand = 'Nitrate'