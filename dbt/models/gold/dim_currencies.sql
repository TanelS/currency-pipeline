{{
    config(
        materialized = 'table',
        indexes=[
            {'columns': ['currency_key'], 'type': "btree"},
            {'columns': ['name'], 'type': "btree"}
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
       thousands_separator
FROM {{ source('silver', 'currencies_stage') }}