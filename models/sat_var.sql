{{ config(
    materialized='table',
    schema='refined',
    post_hook=[
        "CREATE INDEX idx_sat_var_var_key ON {{ this }} (var_key)",
        "CREATE INDEX idx_sat_var_var_group ON {{ this }} (var_group)",
        "CREATE INDEX idx_sat_var_var_label ON {{ this }} (var_label)",
        "CREATE INDEX idx_sat_var_concept ON {{ this }} (concept)",
        "CREATE INDEX idx_sat_var_predicate_type ON {{ this }} (predicate_type)",
        "CREATE INDEX idx_sat_var_survey ON {{ this }} (survey)"
    ]
) }}

SELECT
    v.var_key,
    r.var_group,
    r.var_label,
    r.concept,
    r.predicate_type,
    r.survey
FROM
    {{ ref('var_ref') }} r
JOIN
    {{ ref('hub_var') }} v ON r.var_id = v.var_id AND r.products @> ARRAY[v.product]
