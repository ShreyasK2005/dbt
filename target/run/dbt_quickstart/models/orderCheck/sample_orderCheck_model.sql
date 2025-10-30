
  
    

create or replace table SANDBOX.Shreyas.sample_orderCheck_model
    
    
    
    as (with src as
    (
        SELECT *
        from snowflake_sample_data.tpch_sf1.orders -- read from raw table (today's data, files, etc.) based on source .yml files
    )

    select *,  -- typically very few transformations
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src
    )
;


  