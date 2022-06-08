SELECT CUST_ID, CUST_NAME, CTRY_NAME, RN,TotalOrderValue 
FROM (
    select LC.CUST_ID, LC.CUST_NAME, LN.CTRY_NAME,SUM(LO.ORDER_AMT) as TotalOrderValue ,
    RANK() over(partition by LN.CTRY_NAME order by TotalOrderValue desc) RN
    from {{ref('stg_customer')}} LC 
    join {{ ref('stg_orders')}}  LO
    on LC.CUST_ID=LO.ORDER_ID
    join {{ ref('stg_nation')}}  LN
    on LC.CTRY_ID=LN.CTRY_ID
    group by LC.CUST_ID,LC.CUST_NAME,LN.CTRY_NAME
    order by CTRY_NAME, RN 
) WHERE RN<=5