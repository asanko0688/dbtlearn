{% snapshot snapshot_customer_subscription %}

{{
    config(

      target_schema=env_var('DBT_SNAPSHOT_SCHEMA'),
      unique_key='SUBSCRIPTION_ID',
      strategy='timestamp',
      updated_at='SRC_LAST_UPDATE_TIME',
    )
}}

select S_SUBSCRIPTIONKEY AS SUBSCRIPTION_ID,
C_CUSTKEY AS CUST_ID,
SUBSCRIPTION_NAME,
EFF_DT AS VALID_FROM,
END_DT AS VALID_TO,
LST_UPDT_DT AS SRC_LAST_UPDATE_TIME 
FROM {{ source('CRM', 'customer_subscription') }}
ORDER BY C_CUSTKEY

{% endsnapshot %}