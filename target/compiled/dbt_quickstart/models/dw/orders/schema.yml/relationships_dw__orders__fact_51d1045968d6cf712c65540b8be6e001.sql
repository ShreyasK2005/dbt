
    
    

with child as (
    select part_key as from_field
    from DEV_DW.Shreyas_ORDERS.fact_order_item
    where part_key is not null
),

parent as (
    select part_key as to_field
    from DEV_DW.Shreyas_ORDERS.dim_part
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


