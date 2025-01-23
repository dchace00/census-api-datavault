{{ config(
    materialized='table',
    schema='refined',
    post_hook=[
        "CREATE INDEX idx_sat_year_geo_var_link_key ON {{ this }} (link_key)",
        "CREATE INDEX idx_sat_year_geo_var_val ON {{ this }} (val)",
        "CREATE INDEX idx_sat_year_geo_var_timestamp ON {{ this }} (timestamp)"
    ]
) }}

SELECT
    l.link_key,
    d.val,
    d.timestamp
FROM
    {{ ref('_data_') }} d
    JOIN {{ ref('hub_var') }} v ON d.var_id = v.var_id AND d.product = v.product
    JOIN {{ ref('hub_geo') }} g ON d.geoid = g.geoid AND d.geoname = g.geoname
    JOIN {{ ref('link_year_geo_var') }} l ON d.year = l.year AND g.geo_key = l.geo_key AND v.var_key = l.var_key
