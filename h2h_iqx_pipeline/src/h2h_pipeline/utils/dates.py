from datetime import datetime


def parse_month(month_str: str) -> datetime:
    """Parse YYYY-MM into a datetime at first of month."""
    return datetime.strptime(month_str, "%Y-%m")
