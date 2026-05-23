{{
    config(
        materialized = 'incremental',
        unique_key = 'date_key',
        indexes=[
            {'columns': ['date_key'], 'type': 'btree'},
            {'columns': ['date'], 'type': 'btree'}
        ]
    )
}}



SELECT DISTINCT
    to_char(date_trunc('minute', rate_date), 'YYYYMMDDHH24MI')::bigint as date_key,
    date_trunc('minute', rate_date) as date
FROM {{ source('silver', 'rates_stage') }}

{% if is_incremental() %}
WHERE rate_date > (SELECT MAX(date) FROM {{ this }})
{% endif %}