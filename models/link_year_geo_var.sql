{{ config(
    materialized='table',
    schema='refined',
    post_hook=[
        "CREATE INDEX idx_link_year_geo_var_year ON {{ this }} (year)",
        "CREATE INDEX idx_link_year_geo_var_geo_key ON {{ this }} (geo_key)",
        "CREATE INDEX idx_link_year_geo_var_var_key ON {{ this }} (var_key)"
    ]
) }}

SELECT
    DISTINCT d.year,
    g.geo_key,
    v.var_key
FROM
    {{ ref('_data_') }} d
    JOIN {{ ref('hub_geo') }} g ON d.geoid = g.geoid AND d.geoname = g.geoname
    JOIN {{ ref('hub_var') }} v ON d.var_id = v.var_id AND d.product = v.product
