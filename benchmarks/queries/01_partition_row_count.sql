select count(*) as row_count
from {catalog}.silver.{dataset}_trips
where year = {year}
  and month = {month}
