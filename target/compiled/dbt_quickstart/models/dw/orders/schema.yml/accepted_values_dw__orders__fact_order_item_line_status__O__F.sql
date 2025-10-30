
    
    

with all_values as (

    select
        line_status as value_field,
        count(*) as n_records

    from DEV_DW.Shreyas_ORDERS.fact_order_item
    group by line_status

)

select *
from all_values
where value_field not in (
    'O','F'
)


