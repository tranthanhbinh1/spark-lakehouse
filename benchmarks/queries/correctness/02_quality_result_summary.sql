with latest as (
    select status,
           row_number() over (
               partition by check_name
               order by processed_at desc, dag_run_id desc
           ) as row_number
    from {catalog}.{quality_namespace}.silver_trip_quality_results
    where dataset = '{dataset}' and year = {year} and month = {month}
)
select status, count(*) as check_count
from latest
where row_number = 1
group by status
order by status
