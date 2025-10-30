
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from DEV_DW.Shreyas_ORDERS.dim_order
    group by order_status

)

select *
from all_values
where value_field not in (
    'O','F','P'
)


