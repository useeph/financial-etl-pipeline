{{ config(materialized='table') }}

with date_spine as (
    {{ dbt.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

calendar as (
    select
        cast(date_day as date)                      as date_key,
        extract(year from date_day)                 as year,
        extract(quarter from date_day)              as quarter,
        extract(month from date_day)                as month,
        to_char(date_day, 'Mon')                    as month_name,
        extract(week from date_day)                 as week_of_year,
        extract(day from date_day)                  as day_of_month,
        extract(dayofweek from date_day)            as day_of_week,
        to_char(date_day, 'Dy')                     as day_name,
        case
            when extract(dayofweek from date_day) in (0, 6) then false
            else true
        end                                         as is_weekday,
        case
            when extract(dayofweek from date_day) in (0, 6) then true
            else false
        end                                         as is_weekend
    from date_spine
)

select * from calendar