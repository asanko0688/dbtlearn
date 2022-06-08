select C.CUST_ID, COUNt(distinct O.Order_Id) AS TOTAL_ORDER_CNT, sum(ORDER_AMT) AS TOTAL_ORDER_VAL,
RANK() OVER(ORDER BY TOTAL_ORDER_VAL DESC ) RN 
from {{ref('stg_customer')}} C join 
{{ ref('stg_orders')}} O
on C.CUST_ID=O.CUST_ID
group by C.CUST_ID ORDER BY 3 DESC