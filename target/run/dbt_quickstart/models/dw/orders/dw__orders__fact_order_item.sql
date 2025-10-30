
  
    

create or replace table DEV_DW.Shreyas_ORDERS.fact_order_item
    
    
    
    as (



with line_items as (
    select * from DEV_ODS.Shreyas_ERP.lineitem
),

orders as (
    select * from DEV_ODS.Shreyas_ERP.orders
),

-- Create surrogate keys for date dimensions
line_items_with_dates as (
    select
        line_items.*,
        dateadd('year',26,to_date(l_shipdate)) as ship_date_key,
        dateadd('year',26,to_date(l_commitdate)) as commit_date_key,
        dateadd('year',26,to_date(l_receiptdate)) as receipt_date_key
    from line_items
),

line_items_with_customers as (
    select 
        line_items_with_dates.*,
        orders.o_custkey as l_custkey
    from line_items_with_dates
    left join orders
        on line_items_with_dates.l_orderkey = orders.o_orderkey
),

final as (
    select
        l_orderkey as order_id,
        sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(l_orderkey as varchar), ''))


     as order_key,
        sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(l_custkey as varchar), ''))


     as customer_key,
        sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(l_partkey as varchar), ''))


     as part_key,
        sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(l_suppkey as varchar), ''))


     as supplier_key,
        l_linenumber as line_number,
        l_quantity as quantity,
        l_extendedprice as extended_price,
        l_discount as discount,
        l_tax as tax,
        l_returnflag as return_flag,
        l_linestatus as line_status,
        ship_date_key,
        commit_date_key,
        receipt_date_key,
        l_shipinstruct as ship_instructions,
        l_shipmode as ship_mode,
        -- Calculate derived columns
        l_extendedprice * (1 - l_discount) as discounted_price,
        l_extendedprice * (1 - l_discount) * (1 + l_tax) as final_price
    from line_items_with_customers
)

select *,
    '' as dw_source_name,
    sysdate() as dw_created_datetime,
    sysdate() as dw_updated_datetime,
    false as dw_is_deleted_flag 
from final
    )
;


  