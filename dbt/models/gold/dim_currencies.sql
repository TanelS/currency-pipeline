{{
    config(
        materialized = 'table',
        columns={
            'valid_from': {'data_type': 'timestamp'},
            'valid_to': {'data_type': 'timestamp'},
            'is_current': {'data_type': 'boolean'}
        },
        indexes=[
            {'columns': ['currency_key'], 'type': 'btree'},
            {'columns': ['name'], 'type': 'btree'}
        ]
    )
}}


SELECT short_code as currency_key,
       name,
       code,
       precision,
       subunit,
       symbol,
       symbol_first,
       decimal_mark,
       thousands_separator,
       dbt_valid_from as valid_from,
       dbt_valid_to as valid_to,
       dbt_valid_to is null as is_current
FROM {{ ref('currencies_snapshot') }}