
  
    

create or replace table DEV_ODS.Shreyas_ERP.partsupp
    
    
    
    as (-- Call base ODS macro
-- Table mode = drop and rebuild table each run



    with src as
    (
        SELECT *
        from snowflake_sample_data.tpch_sf1.partsupp
    )

    select *,
        

    

        
        

        
        

    md5(cast(coalesce(cast(PS_SUPPKEY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(PS_AVAILQTY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(PS_SUPPLYCOST as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(PS_COMMENT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as change_hash,
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src
    )
;


  