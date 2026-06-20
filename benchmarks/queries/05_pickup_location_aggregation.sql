select pickup_location_id, count(*) as trip_count, sum(total_amount) as total_amount
from {catalog}.silver.{dataset}_trips
where year = {year} and month = {month}
group by pickup_location_id
