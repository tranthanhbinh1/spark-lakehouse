select count(*) as row_count
from {catalog}.{silver_namespace}.{dataset}_trips
where year = {year} and month = {month}
