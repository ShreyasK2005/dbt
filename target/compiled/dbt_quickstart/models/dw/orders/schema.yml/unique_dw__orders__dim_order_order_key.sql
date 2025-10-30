
    
    

select
    order_key as unique_field,
    count(*) as n_records

from DEV_DW.Shreyas_ORDERS.dim_order
where order_key is not null
group by order_key
having count(*) > 1


