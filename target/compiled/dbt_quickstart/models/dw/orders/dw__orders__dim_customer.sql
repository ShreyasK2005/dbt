




with customers as (
    select * from DEV_ODS.Shreyas_ERP.customer
),

nations as (
    select * from DEV_ODS.Shreyas_ERP.nation
),

regions as (
    select * from DEV_ODS.Shreyas_ERP.region
),

final as (
    select
        
    

    sha2(coalesce(cast('erp' as varchar), '') || '-' || coalesce(cast(customers.c_custkey as varchar), '')) as customer_key


    ,
        customers.c_custkey as customer_id,
        customers.c_name as customer_name,
        customers.c_address as address,
        customers.c_phone as phone,
        customers.c_acctbal as account_balance,
        customers.c_mktsegment as market_segment,
        nations.n_nationkey as nation_key,
        nations.n_name as nation_name,
        regions.r_regionkey as region_key,
        regions.r_name as region_name
    from customers
    left join nations on customers.c_nationkey = nations.n_nationkey
    left join regions on nations.n_regionkey = regions.r_regionkey
)

select *,
    '' as dw_source_name,
    sysdate() as dw_created_datetime,
    sysdate() as dw_updated_datetime,
    false as dw_is_deleted_flag
from final