{{ config(
    alias=get_model_alias(this.name,2),
    tags=["facts"]
) }}

{% set source_system = 'erp' %}

with orders as (
    select * from {{ ref('ods__erp__orders') }}
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
        {{ dw_dim_key_lkp(source_system, ['o_orderkey']) }} as order_key,
        {{ dw_dim_key_lkp(source_system, ['o_custkey']) }} as customer_key,
        order_date_key,
        1 as order_count,
        o_totalprice as total_price
    from order_dates
)

select *,
    {{ dw_audit_columns() }}
from final