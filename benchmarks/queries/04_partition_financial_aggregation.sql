select sum(total_amount) as total_amount, avg(trip_distance) as avg_trip_distance, avg(trip_duration_min) as avg_trip_duration_min
from {catalog}.silver.{dataset}_trips
where year = {year} and month = {month}
