select count(*) as row_count
from lakehouse.silver.{dataset}_trips
where year = {year}
  and month = {month}
