from datetime import datetime


def parse_month(month_str: str) -> datetime:
    """Parse YYYY-MM (or YYYY-MM-DD) into a datetime."""
    try:
        return datetime.strptime(month_str, "%Y-%m")
    except ValueError:
        return datetime.strptime(month_str, "%Y-%m-%d")


def parse_date(date_str: str) -> datetime:
    """Parse a run/output date in YYYY-MM-DD or YYYY-MM format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return datetime.strptime(date_str, "%Y-%m")
