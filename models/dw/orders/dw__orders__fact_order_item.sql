{{ config(
    alias=get_model_alias(this.name,2),
    tags=["facts"]
) }}

{% set source_system = 'erp' %}

with line_items as (
    select * from {{ ref('ods__erp__lineitem') }}
),

orders as (
    select * from {{ ref('ods__erp__orders') }}
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
        {{ dw_dim_key_lkp(source_system, ['l_orderkey']) }} as order_key,
        {{ dw_dim_key_lkp(source_system, ['l_custkey']) }} as customer_key,
        {{ dw_dim_key_lkp(source_system, ['l_partkey']) }} as part_key,
        {{ dw_dim_key_lkp(source_system, ['l_suppkey']) }} as supplier_key,
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
    {{ dw_audit_columns() }} 
from final