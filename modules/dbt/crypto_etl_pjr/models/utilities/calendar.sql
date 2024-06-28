with 
    dates_raw as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/1900', 'mm/dd/yyyy')",
        end_date="to_date('01/01/2200', 'mm/dd/yyyy')"
        )
    }}
)
select * from dates_raw
