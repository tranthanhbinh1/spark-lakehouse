import requests
import os
from multiprocessing import Pool


def _execute_task(args):
    func, month, year = args
    func(month, year)
    return True


def _print_progress(completed, total):
    percent = (completed / total) * 100 if total else 100
    print(
        f"\rDownloading data: {completed}/{total} ({percent:.0f}%)", end="", flush=True
    )


def fetch_yellow_trips_initial_data(month: str, year: int):
    """Download NYC TLC Trips data for a given month and year."""
    yellow_trip_data_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    response = requests.get(yellow_trip_data_base_url.format(year=year, month=month))
    file_path = f"data/{year}/yellow_tripdata_{year}-{month}.parquet"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(response.content)


def fetch_green_trips_initial_data(month: str, year: int):
    """Download NYC TLC Green Trips data for a given month and year."""
    green_trip_data_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"
    url = green_trip_data_base_url.format(year=year, month=month)
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 403:
            print(f"Skipping green trips for {year}-{month}: not available")
            return
        raise

    file_path = f"data/{year}/green_tripdata_{year}-{month}.parquet"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(response.content)


def fetch_fhv_trips_initial_data(month: str, year: int):
    """Download NYC TLC FHV (For Hire Vehicle) Trips data for a given month and year."""
    fhv_trip_data_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet"
    url = fhv_trip_data_base_url.format(year=year, month=month)
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 403:
            print(f"Skipping FHV trips for {year}-{month}: not available")
            return
        raise

    file_path = f"data/{year}/fhv_tripdata_{year}-{month}.parquet"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(response.content)


def fetch_fhvhv_trips_initial_data(month: str, year: int):
    """Download NYC TLC HVFHV (High Volume For Hire Vehicle) Trips data for a given month and year."""
    fhvhv_trip_data_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet"
    url = fhvhv_trip_data_base_url.format(year=year, month=month)
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 403:
            print(f"Skipping HVFHV trips for {year}-{month}: not available")
            return
        raise

    file_path = f"data/{year}/fhvhv_tripdata_{year}-{month}.parquet"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(response.content)


def download_all_data_for_year(year: int):
    """Download all TLC trip data for a given year."""
    months = [f"{i:02d}" for i in range(1, 13)]
    tasks = []

    for month in months:
        tasks.append((fetch_yellow_trips_initial_data, month, year))
        tasks.append((fetch_green_trips_initial_data, month, year))
        tasks.append((fetch_fhv_trips_initial_data, month, year))
        tasks.append((fetch_fhvhv_trips_initial_data, month, year))

    total_tasks = len(tasks)
    completed = 0

    with Pool(processes=8) as pool:
        for _ in pool.imap_unordered(_execute_task, tasks):
            completed += 1
            _print_progress(completed, total_tasks)

    print()


if __name__ == "__main__":
    year_to_download = 2025
    download_all_data_for_year(year_to_download)
