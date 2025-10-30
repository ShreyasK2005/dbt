
  
    

create or replace table SANDBOX.Shreyas_SAMPLE.same_relation_model
    
    
    
    as (
    SELECT *
    FROM DW.ORDERS.FACT_ORDER AS l
    JOIN DW.ORDERS.DIM_ORDER AS r
        ON l.ORDER_ID = r.ORDER_ID

    )
;


  