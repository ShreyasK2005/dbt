{{ config(
    alias=get_model_alias(this.name,2),
    tags=["dimensions"]
) }}

{% set source_system = 'erp' %}
{% set source_key_columns = ['p_partkey'] %}

with parts as (
    select * from {{ ref('ods__erp__part') }}
),

final as (
    select
        {{ dw_create_dim_key(source_system, source_key_columns) }},
        p_partkey as part_id,
        p_name as part_name,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_type as type,
        p_size as size,
        p_container as container,
        p_retailprice as retail_price
    from parts
)

select *,
    {{ dw_audit_columns() }}
from final