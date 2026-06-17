select status, count(*) as check_count
from lakehouse.quality.silver_trip_quality_results
where dataset = '{dataset}'
  and year = {year}
  and month = {month}
group by status
order by status
