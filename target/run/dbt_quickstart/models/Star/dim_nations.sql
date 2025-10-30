
  create or replace   view SANDBOX.Shreyas.dim_nations
  
  
  
  
  as (
    with src as (
  select * from snowflake_sample_data.tpch_sf1.nation
)
select
  N_NATIONKEY as nation_id,
  N_NAME      as nation_name,
  N_REGIONKEY as region_id
from src
  );

