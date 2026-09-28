SELECT countryCode AS country, determinand, unit, year,
       median_value, avg_value, site_count, measurement_count
FROM dbw_medallion_water.gold.yearly_by_country_determinand
WHERE site_count >= 5          -- ignore countries with very few sites