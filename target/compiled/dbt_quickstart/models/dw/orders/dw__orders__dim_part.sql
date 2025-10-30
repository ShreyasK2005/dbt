




with parts as (
    select * from DEV_ODS.Shreyas_ERP.part
),

final as (
    select
        
    

    sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(p_partkey as varchar), '')) as part_key


    ,
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
    '' as dw_source_name,
    sysdate() as dw_created_datetime,
    sysdate() as dw_updated_datetime,
    false as dw_is_deleted_flag
from final