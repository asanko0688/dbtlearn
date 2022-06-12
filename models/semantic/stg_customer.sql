


select 
C_CUSTKEY AS CUST_ID,
C_NAME AS CUST_NAME ,
C_ADDRESS AS CUST_ADDRESS,
C_PHONE AS CUST_PHONE,
C_ACCTBAL AS CUST_ACCNT_BAL,
C_NATIONKEY AS CTRY_ID,
md5(C_NAME||C_ADDRESS||C_PHONE||C_ACCTBAL||C_NATIONKEY) AS HASH_VAL,
CURRENT_TIMESTAMP AS LOADDATETIME
from {{ source('CRM', 'customer') }}
ORDER BY CUST_ID --


