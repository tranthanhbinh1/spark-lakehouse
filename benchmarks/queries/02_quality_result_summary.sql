select status, count(*) as check_count
from {catalog}.quality.silver_trip_quality_results
where benchmark_run_id = '{benchmark_run_id}'
  and dataset = '{dataset}'
  and year = {year}
  and month = {month}
group by status
order by status
