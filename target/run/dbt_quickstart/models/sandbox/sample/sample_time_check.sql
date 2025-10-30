
  
    

create or replace table SANDBOX.Shreyas_SAMPLE.sample_time_check
    
    
    
    as (
        

    with left_table as (
        select * from snowflake_sample_data.tpch_sf1.customer
    ),
    right_table as (
        select * from snowflake_sample_data.tpch_sf1.orders
    )
    select *
    from left_table l
    join right_table r
        on l.C_CUSTKEY = r.O_CUSTKEY

    )
;


  