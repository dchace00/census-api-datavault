{{ config(
    materialized='table',
    schema='refined',
    post_hook="CREATE INDEX idx_hub_year_year ON {{ this }} (year)"
) }}

SELECT
    DISTINCT year
FROM
    {{ ref('_data_') }}
