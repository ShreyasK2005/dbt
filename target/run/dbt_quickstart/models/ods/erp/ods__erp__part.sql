
  
    

create or replace table DEV_ODS.Shreyas_ERP.part
    
    
    
    as (-- Call base ODS macro
-- Table mode = drop and rebuild table each run



    with src as
    (
        SELECT *
        from snowflake_sample_data.tpch_sf1.part
    )

    select *,
        

    

        
        

        
        

    md5(cast(coalesce(cast(P_NAME as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(P_MFGR as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(P_BRAND as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(P_TYPE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(P_SIZE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(P_CONTAINER as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(P_RETAILPRICE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(P_COMMENT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as change_hash,
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src
    )
;


  