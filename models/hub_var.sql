{{ config(
    materialized='table',
    schema='refined',
    post_hook=[
        "CREATE INDEX idx_hub_var_var_id ON {{ this }} (var_id)",
        "CREATE INDEX idx_hub_var_product ON {{ this }} (product)"
    ]
) }}

WITH ranked_var AS (
    SELECT 
        DISTINCT var_id,
        product,
        ROW_NUMBER() OVER (ORDER BY var_id) AS var_key
    FROM
        {{ ref('_data_') }}
)

SELECT
    var_key,
    var_id,
    product
FROM
    ranked_var
