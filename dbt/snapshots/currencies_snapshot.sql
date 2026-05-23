{% snapshot currencies_snapshot %}

{{
    config(
        target_schema='public',
        unique_key='short_code',
        strategy='check',
        check_cols='all'
    )
}}

SELECT * FROM {{ source('silver', 'currencies_stage') }}

{% endsnapshot %}

