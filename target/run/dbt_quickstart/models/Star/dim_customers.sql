
  create or replace   view SANDBOX.Shreyas.dim_customers
  
  
  
  
  as (
    with src as (
  select * from snowflake_sample_data.tpch_sf1.customer
)
select
  C_CUSTKEY   as customer_id,
  C_NAME      as customer_name,
  C_ADDRESS   as address,
  C_NATIONKEY as nation_id
from src
  );

