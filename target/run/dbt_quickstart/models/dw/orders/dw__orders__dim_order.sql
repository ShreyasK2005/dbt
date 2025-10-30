
  
    

create or replace table DEV_DW.Shreyas_ORDERS.dim_order
    
    
    
    as (




with orders as (
    select * from DEV_ODS.Shreyas_ERP.orders
),

-- Create surrogate keys for date dimensions
order_dates as (
    select
        orders.*,
        dateadd('year',26,to_date(o_orderdate)) as order_date
    from orders
),

final as (
    select
        
    

    sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(o_orderkey as varchar), '')) as order_key


    ,
        o_orderkey as order_id,
        order_date,
        o_orderstatus as order_status,
        o_orderpriority as order_priority,
        o_clerk as clerk,
        o_shippriority as ship_priority
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


  