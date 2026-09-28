SELECT determinand, unit, water_body_type, year,
       median_value, site_count, measurement_count
FROM dbw_medallion_water.gold.by_water_body_type
WHERE water_body_type <> 'Unknown'
  AND site_count >= 5