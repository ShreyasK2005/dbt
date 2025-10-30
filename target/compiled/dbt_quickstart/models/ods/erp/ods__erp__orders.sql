-- Call base ODS macro
-- Table mode = drop and rebuild table each run



    with src as
    (
        SELECT *
        from snowflake_sample_data.tpch_sf1.orders
    )

    select *,
        

    

        
        

        
        

    md5(cast(coalesce(cast(O_CUSTKEY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(O_ORDERSTATUS as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(O_TOTALPRICE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(O_ORDERDATE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(O_ORDERPRIORITY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(O_CLERK as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(O_SHIPPRIORITY as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(O_COMMENT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as change_hash,
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src