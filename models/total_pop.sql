{{ config(
    materialized='table',
    schema='report'
) }}

SELECT 
    l.link_key, 
    l.year, 
    l.geo_key, 
    l.var_key, 
    sv.var_label, 
    s.val AS count
FROM 
    {{ ref('sat_year_geo_var') }} s
JOIN 
    {{ ref('link_year_geo_var') }} l ON s.link_key = l.link_key 
JOIN 
    {{ ref('sat_var') }} sv ON l.var_key = sv.var_key
JOIN 
    {{ ref('hub_var') }} hv ON l.var_key = hv.var_key 
WHERE 
    hv.var_id IN (
        -- Total Population variables
        'S0101_C01_001E'
    )
