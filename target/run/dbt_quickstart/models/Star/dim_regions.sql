
  create or replace   view SANDBOX.Shreyas.dim_regions
  
  
  
  
  as (
    with src as (
  select * from snowflake_sample_data.tpch_sf1.region
)
select
  R_REGIONKEY as region_id,
  R_NAME      as region_name
from src
  );

