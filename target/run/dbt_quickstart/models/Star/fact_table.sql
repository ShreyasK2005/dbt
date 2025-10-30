
  create or replace   view SANDBOX.Shreyas.fact_table
  
  
  
  
  as (
    with src as (
  select * from snowflake_sample_data.tpch_sf1.orders
)
select
  O_ORDERKEY   as order_id,
  O_CUSTKEY    as customer_id,
  O_ORDERDATE  as order_date,
  O_TOTALPRICE as total_price
from src
  );

