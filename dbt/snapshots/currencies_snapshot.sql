{% snapshot currencies_snapshot %}

{{
    config(
        target_schema='public',
        unique_key='short_code',
        strategy='check',
        check_cols=['name', 'code', 'precision', 'subunit', 'symbol', 'symbol_first', 'decimal_mark', 'thousands_separator']
    )
}}

SELECT * FROM {{ source('silver', 'currencies_stage') }}

{% endsnapshot %}

