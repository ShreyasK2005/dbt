{{ config(
    alias=get_model_alias(this.name,2),
    tags=["dimensions"]
) }}

{% set source_system = 'erp' %}
{% set source_key_columns = ['customers.c_custkey'] %}

with customers as (
    select * from {{ ref('ods__erp__customer') }}
),

nations as (
    select * from {{ ref('ods__erp__nation') }}
),

regions as (
    select * from {{ ref('ods__erp__region') }}
),

final as (
    select
        {{ dw_create_dim_key(source_system, source_key_columns) }},
        customers.c_custkey as customer_id,
        customers.c_name as customer_name,
        customers.c_address as address,
        customers.c_phone as phone,
        customers.c_acctbal as account_balance,
        customers.c_mktsegment as market_segment,
        nations.n_nationkey as nation_key,
        nations.n_name as nation_name,
        regions.r_regionkey as region_key,
        regions.r_name as region_name
    from customers
    left join nations on customers.c_nationkey = nations.n_nationkey
    left join regions on nations.n_regionkey = regions.r_regionkey
)

select *,
    {{ dw_audit_columns() }}
from final