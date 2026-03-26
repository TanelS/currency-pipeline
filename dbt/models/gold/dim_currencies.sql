{{
    config(
        materialized = 'table'
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