
  
    

create or replace table DEV_DW.Shreyas_ORDERS.dim_supplier
    
    
    
    as (




with suppliers as (
    select * from DEV_ODS.Shreyas_ERP.supplier
),

nations as (
    select * from DEV_ODS.Shreyas_ERP.nation
),

regions as (
    select * from DEV_ODS.Shreyas_ERP.region
),

final as (
    select
        
    

    sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(suppliers.s_suppkey as varchar), '')) as supplier_key


    ,
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
    '' as dw_source_name,
    sysdate() as dw_created_datetime,
    sysdate() as dw_updated_datetime,
    false as dw_is_deleted_flag
from final
    )
;


  