
  
    

create or replace table DEV_DW.Shreyas_ORDERS.fact_order
    
    
    
    as (



with orders as (
    select * from DEV_ODS.Shreyas_ERP.orders
),

-- Create surrogate keys for date dimensions
order_dates as (
    select
        orders.*,
        dateadd('year',26,to_date(o_orderdate)) as order_date_key
    from orders
),

final as (
    select
        o_orderkey as order_id,
        sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(o_orderkey as varchar), ''))


     as order_key,
        sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(o_custkey as varchar), ''))


     as customer_key,
        order_date_key,
        1 as order_count,
        o_totalprice as total_price
    from order_dates
)

select *,
    '' as dw_source_name,
    sysdate() as dw_created_datetime,
    sysdate() as dw_updated_datetime,
    false as dw_is_deleted_flag
from final
    )
;


  