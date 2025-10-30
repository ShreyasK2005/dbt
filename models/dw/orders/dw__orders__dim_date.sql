{{ config(
    alias=get_model_alias(this.name,2),
    materialized='table',
    tags=["dimensions"]
) }}

{% set source_system = 'dbt' %}

with date_spine as (
    {{  
        dbt_utils.date_spine(
        datepart='day',
        start_date="to_date('2018-01-01', 'yyyy-mm-dd')",
        end_date="to_date(concat(year(current_date) + 1,'-12-31'))"
    )}}
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
    {{ dw_audit_columns() }} 
from dates