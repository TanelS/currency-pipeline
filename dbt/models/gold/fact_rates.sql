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
    SELECT date_key, date FROM {{ ref('dim_date') }}
),

{% if is_incremental() %}
cutoff_date AS (
    SELECT MAX(d2.date) as cutoff_date
    FROM {{ this }} f
    JOIN dates d2 ON f.date_key = d2.date_key
), -- placement of the comma here is important!
{% endif %}

final AS (
    SELECT
        d.date_key,
        r.curr_base,
        r.currency,
        r.rate
    FROM {{ source('silver', 'rates_stage') }} r
    LEFT JOIN dates d ON date_trunc('minute', r.rate_date) = d.date

    {% if is_incremental() %}
    WHERE r.rate_date > (SELECT cutoff_date FROM cutoff_date)
    {% endif %}
)

SELECT * FROM final