{{
    config(
        materialized = 'table'
    )
}}



SELECT DISTINCT to_char(rate_date, 'YYYYMMDDHHMMSS')::bigint as date_key,
       rate_date as date
from  {{ source('silver', 'rates_stage') }}


