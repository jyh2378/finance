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


def find_closest_date_row(df, date):
    time_diff = df.index - pd.to_datetime(pd.to_datetime(date).replace(tzinfo=df.index.tzinfo))