{{ config(
    alias=get_model_alias(this.name,2),
    tags=["dimensions"]
) }}

{% set source_system = 'erp' %}
{% set source_key_columns = ['suppliers.s_suppkey'] %}

with suppliers as (
    select * from {{ ref('ods__erp__supplier') }}
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
        suppliers.s_suppkey as supplier_id,
        suppliers.s_name as supplier_name,
        suppliers.s_address as address,
        suppliers.s_phone as phone,
        suppliers.s_acctbal as account_balance,
        nations.n_nationkey as nation_key,
        nations.n_name as nation_name,
        regions.r_regionkey as region_key,
        regions.r_name as region_name
    from suppliers
    left join nations on suppliers.s_nationkey = nations.n_nationkey
    left join regions on nations.n_regionkey = regions.r_regionkey
)

select *,
    {{ dw_audit_columns() }}
from final