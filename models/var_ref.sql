{{ config(
    materialized='table',
    schema='stage'
) }}

SELECT DISTINCT
    j.var_id,
    j.var_group,
    j.var_label,
    j.concept,
    j.predicate_type,
    j.survey,
    array_agg(DISTINCT j.product) AS products,
    array_agg(DISTINCT j.year) AS years,
    array_agg(DISTINCT j.geounit) AS geounits,
    COUNT(DISTINCT j.year || j.geounit || j.product || j.var_id) AS datasets
FROM (
    SELECT 
        d.year,
        d.geoid,
        d.geoname,
        d.geounit,
        d.timestamp,
        d.var_id,
        d.val,
        d.survey,
        d.product,
        l.var_label,
        l.concept,
        l.var_group,
        l.predicate_type
    FROM 
        {{ ref('var_labels') }} l
    JOIN 
        {{ ref('_data_') }} d ON l.var_id = d.var_id
) j
GROUP BY 
    j.var_id, j.var_group, j.var_label, j.concept, j.predicate_type, j.survey
ORDER BY 
    j.var_id