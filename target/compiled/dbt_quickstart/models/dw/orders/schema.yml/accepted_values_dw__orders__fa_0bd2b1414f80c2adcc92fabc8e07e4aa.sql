
    
    

with all_values as (

    select
        return_flag as value_field,
        count(*) as n_records

    from DEV_DW.Shreyas_ORDERS.fact_order_item
    group by return_flag

)

select *
from all_values
where value_field not in (
    'R','A','N'
)


