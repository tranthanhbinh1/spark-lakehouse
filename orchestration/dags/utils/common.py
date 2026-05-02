def parse_partition(value: str) -> tuple[int, int]:
    year, month = value.split("-", maxsplit=1)
    return int(year), int(month)


def format_partition(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def next_partition(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1
