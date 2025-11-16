import datetime

import pandas as pd


def get_today(before_day=0, to_str=False, str_format="%Y-%m-%d"):
    today = datetime.date.today() - datetime.timedelta(days=before_day)
    if to_str:
        today = today.strftime(str_format)
    return today


def get_quarter(date: datetime.date | pd.Timestamp | str):
    if isinstance(date, str):
        date = datetime.date.strptime(date)
    elif isinstance(date, pd.Timestamp):
        date = date.date()
    quarter = [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]
    return quarter[date.month - 1]


def get_quarter_end_date(quarter: int):
    assert 0 < quarter < 5, "quarter need to be in 1 to 4"
    year = datetime.date.today().year
    quarter_end_date = [
        datetime.date(year=year, month=3, day=31).strftime("%Y-%m-%d"),
        datetime.date(year=year, month=6, day=30).strftime("%Y-%m-%d"),
        datetime.date(year=year, month=9, day=30).strftime("%Y-%m-%d"),
        datetime.date(year=year, month=12, day=31).strftime("%Y-%m-%d")
    ]
    return quarter_end_date[quarter - 1]


def find_closest_before_date(
        datetimeindex: pd.DatetimeIndex, target_date: str | pd.Timestamp, strptime_fmt: str | None = None
    ):
    # Find the closest date in the DatetimeIndex to the target date
    if isinstance(target_date, str):
        target_date = pd.to_datetime(target_date, format=strptime_fmt)
    target_date = target_date.tz_localize(datetimeindex.tz)

    closest_date = datetimeindex[datetimeindex < target_date].max()
    if pd.isna(closest_date):
        return None
    else:
        return closest_date
