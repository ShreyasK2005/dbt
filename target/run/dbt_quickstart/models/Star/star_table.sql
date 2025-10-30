
  create or replace   view SANDBOX.Shreyas.star_table
  
  
  
  
  as (
    with f as (select * from SANDBOX.Shreyas.fact_table),
     c as (select * from SANDBOX.Shreyas.dim_customers),
     n as (select * from SANDBOX.Shreyas.dim_nations),
     r as (select * from SANDBOX.Shreyas.dim_regions)

select
  f.order_id,
  f.order_date,
  f.total_price,
  c.customer_id,
  c.customer_name,
  n.nation_name,
  r.region_name
from f
join c on f.customer_id = c.customer_id
left join n on c.nation_id = n.nation_id
left join r on n.region_id = r.region_id
  );

