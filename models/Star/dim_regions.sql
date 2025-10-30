with src as (
  select * from {{ source('erp','region') }}
)
select
  R_REGIONKEY as region_id,
  R_NAME      as region_name
from src
