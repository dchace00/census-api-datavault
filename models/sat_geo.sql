{{ config(
    materialized='table',
    schema='refined',
    post_hook="CREATE INDEX idx_sat_geo_geo_key ON {{ this }} (geo_key)"
) }}

SELECT
    g.geo_key,
    d.geounit
FROM
    {{ ref('_data_') }} d
    JOIN {{ ref('hub_geo') }} g ON d.geoid = g.geoid AND d.geoname = g.geoname
GROUP BY
    g.geo_key,
    d.geounit
