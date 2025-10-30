



with date_spine as (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
    
    

    )

    select *
    from unioned
    where generated_number <= 3286
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('2018-01-01', 'yyyy-mm-dd')
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date(concat(year(current_date) + 1,'-12-31'))

)

select * from filtered


),

dates as (
    SELECT
        date_day::date AS date_key,
        date_day AS full_date,
        YEAR(date_day) AS year_nbr,
        MONTH(date_day) AS month_nbr,
        MONTHNAME(date_day) AS month_name,
        DAY(date_day) AS day_of_month,
        DAYOFWEEK(date_day) AS day_of_week_nbr,
        DAYNAME(date_day) AS day_of_week_name,
        WEEKOFYEAR(date_day) AS week_of_year_nbr,
        QUARTER(date_day) AS quarter_nbr,
        'Q' || QUARTER(date_day) || '-' || YEAR(date_day) AS quarter_name,
        TO_CHAR(date_day, 'YYYYMM') AS year_month_nbr,
        TO_CHAR(date_day, 'YYYY') || '-' || TO_CHAR(date_day, 'MM') AS year_month_name,
        YEAR(date_day)::VARCHAR ||  WEEKOFYEAR(date_day)::VARCHAR as year_week_nbr,
        LAST_DAY(date_day) = date_day as is_last_day_of_month_flag,
        DATE_TRUNC('month', date_day) = date_day as is_first_day_of_month_flag,
        CASE DAYOFWEEK(date_day)
            WHEN 1 THEN TRUE
            ELSE FALSE
        END as is_sunday_flag,
         CASE DAYOFWEEK(date_day)
            WHEN 7 THEN TRUE
            ELSE FALSE
        END as is_saturday_flag,
        CASE DAYOFWEEK(date_day)
            WHEN 1 THEN FALSE
            WHEN 7 THEN FALSE
            ELSE TRUE
        END as is_weekday_flag,
        CASE DAYOFWEEK(date_day)
            WHEN 1 THEN TRUE
            WHEN 7 THEN TRUE
            ELSE FALSE
        END as is_weekend_flag
    FROM date_spine
)

select *,
    '' as dw_source_name,
    sysdate() as dw_created_datetime,
    sysdate() as dw_updated_datetime,
    false as dw_is_deleted_flag 
from dates