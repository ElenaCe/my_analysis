WITH 
sea_level_group AS (
    SELECT 
        CASE WHEN elevation_ft < 0 THEN 'under_sea_level' 
             WHEN elevation_ft >= 0 THEN 'above_sea_level'
             ELSE 'extra'
        END AS sea_level_elevation,
        country
    FROM volcano_schema.volcano_regional_data
),
sea_level_count AS (
    SELECT 
        COUNT(*) AS number_of_each_type,
        sea_level_elevation,
        country
    FROM sea_level_group
    GROUP BY sea_level_elevation, country
),
country_select AS (
    SELECT
        country
    FROM sea_level_count
    WHERE number_of_each_type >= 10
)
SELECT 
    country,
    COALESCE(SUM(CASE WHEN sea_level_elevation = 'under_sea_level' THEN number_of_each_type ELSE 0 END), 0) AS under_sea_level_count,
    COALESCE(SUM(CASE WHEN sea_level_elevation = 'above_sea_level' THEN number_of_each_type ELSE 0 END), 0) AS above_sea_level_count
FROM sea_level_count
WHERE country IN (SELECT country FROM country_select)
GROUP BY country;