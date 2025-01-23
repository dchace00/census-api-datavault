{{ config(
    materialized='table',
    schema='refined',
    post_hook=[
        "CREATE INDEX idx_hub_geo_geoid ON {{ this }} (geoid)",
        "CREATE INDEX idx_hub_geo_geoname ON {{ this }} (geoname)"
    ]
) }}

WITH ranked_geo AS (
    SELECT 
        DISTINCT geoid,
        geoname,
        ROW_NUMBER() OVER (ORDER BY geoid) AS geo_key
    FROM
        {{ ref('_data_') }}
)

SELECT
    geo_key,
    geoid,
    geoname
FROM
    ranked_geo
