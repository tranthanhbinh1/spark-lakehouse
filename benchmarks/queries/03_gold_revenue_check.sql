select trip_count, valid_trip_count, revenue_trip_count, total_amount_sum, avg_trip_distance, avg_trip_duration_min
from {catalog}.gold.trip_revenue_monthly
where dataset = '{dataset}' and year = {year} and month = {month}
