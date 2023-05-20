from datetime import datetime as dt


def convert_HR_column(x: str) -> int:
    """Takes a value in a pandas column, converts str to int

    Args:
        x (str): heart rate value

    Returns:
        x (int): heart rate as an int
    """
    if isinstance(x, str):
        return int(x)
    return x


def convert_power_column(x: str) -> float:
    """Takes a value in a pandas column, converts str to int

    Args:
        x (str): power value

    Returns:
        x (float): power as a float
    """
    if isinstance(x, str):
        return float(x)
    return x


def get_today_date() -> str:
    """ " Function returns todays date as a string

    Returns:
        date (str): Today's date
    """
    return dt.today().strftime("%Y-%m-%d")
