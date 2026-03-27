{{
    config(
        materialized = 'incremental',
        unique_key = 'date_key',
        indexes=[
            {'columns': ['date_key'], 'type': "btree"},
            {'columns': ['date'], 'type': "btree"}
        ]
    )
}}



SELECT DISTINCT
    to_char(rate_date, 'YYYYMMDDHHMMSS')::bigint as date_key,
    rate_date as date
FROM {{ source('silver', 'rates_stage') }}

{% if is_incremental() %}
WHERE rate_date > (SELECT MAX(date) FROM {{ this }})
{% endif %}