-- Call base ODS macro
-- Table mode = drop and rebuild table each run


    with src as
    (
        SELECT *
        from snowflake_sample_data.tpch_sf1.lineitem
    )

    select *,
        

    

        
            
        
            
        

    md5(cast(coalesce(cast(L_PARTKEY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_SUPPKEY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_QUANTITY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_EXTENDEDPRICE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_DISCOUNT as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_TAX as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_RETURNFLAG as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_LINESTATUS as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_SHIPDATE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_COMMITDATE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_RECEIPTDATE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_SHIPINSTRUCT as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_SHIPMODE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(L_COMMENT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as change_hash,
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src