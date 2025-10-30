
    
    

select
    part_key as unique_field,
    count(*) as n_records

from DEV_DW.Shreyas_ORDERS.dim_part
where part_key is not null
group by part_key
having count(*) > 1


