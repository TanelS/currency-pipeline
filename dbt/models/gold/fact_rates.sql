{{
    config(
        materialized = 'incremental',
        unique_key = ['date_key', 'curr_base', 'currency'],
        indexes=[
            {'columns': ['date_key', 'curr_base', 'currency'], 'type': 'btree'},
            {'columns': ['date_key'], 'type': 'btree'}
        ]
    )
}}

WITH dates AS (
    SELECT
       date_key,
       date
    FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        d.date_key,
        r.curr_base,
        r.currency,
        r.rate
    FROM {{ source('silver', 'rates_stage') }} r
    LEFT JOIN dates d ON r.rate_date = d.date

    {% if is_incremental() %}
    WHERE r.rate_date > (
        SELECT MAX(d2.date)
        FROM {{ this }} f
        JOIN {{ ref('dim_date') }} d2 ON f.date_key = d2.date_key
    )
    {% endif %}
)

SELECT * FROM final