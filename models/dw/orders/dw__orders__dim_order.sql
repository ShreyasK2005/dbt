{{ config(
    alias=get_model_alias(this.name,2),
    tags=["dimensions"]
) }}

{% set source_system = 'erp' %}
{% set source_key_columns = ['o_orderkey'] %}

with orders as (
    select * from {{ ref('ods__erp__orders') }}
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
        {{ dw_create_dim_key(source_system, source_key_columns) }},
        o_orderkey as order_id,
        order_date,
        o_orderstatus as order_status,
        o_orderpriority as order_priority,
        o_clerk as clerk,
        o_shippriority as ship_priority
    from order_dates
)

select *,
    {{ dw_audit_columns() }}
from final