-- Call base ODS macro
-- Table mode = drop and rebuild table each run



    with src as
    (
        SELECT *
        from snowflake_sample_data.tpch_sf1.supplier
    )

    select *,
        

    

        
        

        
        

    md5(cast(coalesce(cast(S_NAME as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(S_ADDRESS as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(S_NATIONKEY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(S_PHONE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(S_ACCTBAL as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(S_COMMENT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as change_hash,
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src