
  
    

create or replace table DEV_ODS.Shreyas_ERP.nation
    
    
    
    as (-- Call base ODS macro
-- Table mode = drop and rebuild table each run



    with src as
    (
        SELECT *
        from snowflake_sample_data.tpch_sf1.nation
    )

    select *,
        

    

        
        

        
        

    md5(cast(coalesce(cast(N_NAME as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(N_REGIONKEY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(N_COMMENT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as change_hash,
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src
    )
;


  