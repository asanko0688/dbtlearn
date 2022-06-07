select C_CUSTKEY AS CUSTID,
C_NAME AS CUST_NAME ,
C_ADDRESS AS CUST_ADDRESS,
C_PHONE AS CUST_PHONE,
C_ACCTBAL AS CUST_ACCNT_BAL
from {{ source('LANDING', 'customer') }}
ORDER BY CUSTID --