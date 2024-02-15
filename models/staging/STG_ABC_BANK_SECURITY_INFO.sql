{{ config(materialized='ephemeral') }}

WITH src_data AS (
    SELECT
        SECURITY_CODE as SECURITY_CODE -- text
        ,SECURITY_NAME as SECURITY_NAME -- text
        ,SECTOR as SECTOR_NAME -- text
        ,INDUSTRY as INDUSTRY_NAME -- text
        ,COUNTRY as COUNTRY_CODE -- text
        ,EXCHANGE as EXCHANGE_CODE -- text
        ,LOAD_TS as LOAD_TS -- timestamp_ntz
        ,'SEED.ABC_Bank_SECURITY_INFO' as RECORD_SOURCE -- text
    FROM   
        {{ source('seeds', 'ABC_Bank_SECURITY_INFO') }}
)

,default_record as (
  SELECT
      '-1'      as SECURITY_CODE
    , 'Missing' as SECURITY_NAME
    , 'Missing' as SECTOR_NAME
    , 'Missing' as INDUSTRY_NAME
    , '-1'      as COUNTRY_CODE
    , '-1'      as EXCHANGE_CODE
    , '2020-01-01'          as LOAD_TS_UTC
    , 'System.DefaultKey'   as RECORD_SOURCE
)

,hashed AS (
    SELECT 
        concat_ws('|', SECURITY_CODE) as SECURITY_HKEY
        ,concat_ws('|', SECURITY_CODE, 
                        SECTOR_NAME,
                        INDUSTRY_NAME,
                        COUNTRY_CODE,
                        EXCHANGE_CODE ) as SECURITY_HDIFF
        , * EXCLUDE LOAD_TS
        ,LOAD_TS as LOAD_TS_UTC
    FROM
        src_data
)

SELECT * FROM hashed