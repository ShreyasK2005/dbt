with src as (
  select * from {{ source('erp','customer') }}
)
select
  C_CUSTKEY   as customer_id,
  C_NAME      as customer_name,
  C_ADDRESS   as address,
  C_NATIONKEY as nation_id
from src
