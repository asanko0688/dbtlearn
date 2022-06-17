{{
  config(
    materialized = "incremental"
  )
}}
SELECT COUNTRY, STATE, CODE,
md5(STATE) AS HASH_VAL,
CURRENT_TIMESTAMP AS LOADDATETIME
FROM {{ source('CRM', 'country_state') }} 