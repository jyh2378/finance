import datetime
import holidays


base_dict = {
    "작년 동분기 순수익": ("Net Income", 5),
    "직전분기 순수익": ("Net Income", 1),
    "현분기 순수익": ("Net Income", 0),
    "작년 동분기 영업이익": ("Operating Income", 5),
    "직전분기 영업이익": ("Operating Income", 1),
    "현분기 영업이익": ("Operating Income", 0),
    "작년 동분기 순매출": ("Total Revenue", 5),
    "직전분기 순매출": ("Total Revenue", 1),
    "현분기 순매출": ("Total Revenue", 0),
    "작년 동분기 영업활동현금흐름": ("Operating Cash Flow", 5),
    "직전분기 영업활동현금흐름": ("Operating Cash Flow", 5),
    "현분기 영업활동현금흐름": ("Operating Cash Flow", 5), 
    "ROE": "ROE",
    "PEGR": "PEGR",
    "PBR": "PBR",
    "PSR": "PSR",
}


def get_recent_5q_dates(base_date: datetime.date):
    last_5q_dates = []
    year = base_date.year
    recent_q = get_quarter(base_date)
    m, d = get_quarter_date(recent_q)
    last_5q_dates.append(datetime.date(year=year, month=m, day=d))
    for i in range(1, 5):
        q = recent_q - i
        if q < 1:
            y = year - 1
            q = q + 4
        else:
            y = year
            q = q
        m, d = get_quarter_date(q)
        last_5q_dates.append(datetime.date(year=y, month=m, day=d))

    return last_5q_dates


def nandiv(numerator, denominator):
    if numerator is not None and denominator is not None:
        return numerator / (denominator + 10e-10)
    else:
        None


def calc_growth_rate(this, prev):
    return (this - prev) / (prev + 10e-10) * 100


def get_quarter(date: datetime.date):
    quarter = [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]
    return quarter[date.month - 1]


def get_quarter_date(quarter):
    date = [(3, 31), (6, 30), (9, 30), (12, 31)]
    return date[quarter - 1]


def calc_last_business_day(date: datetime.date, country="us"):
    if country == "us":
        holiday_list = holidays.US(years=date.year)
    elif country == "kr":
        holiday_list = holidays.Korea(years=date.year)
    else:
        raise NotImplementedError
    while date in holiday_list or date.isoweekday() > 5:
        date -= datetime.timedelta(days=1)
    return date


def get_today(before_day=0):
    today = datetime.date.today() - datetime.timedelta(days=before_day)
    return today


def normalize_ticker(tickers):
    new_tickers = []
    for ticker in tickers:
        if ticker == "BRK/A":
            new_tickers.append("BRK-A")
        if ticker == "BRK/B":
            new_tickers.append("BRK-B")
        else:
            new_tickers.append(ticker)
    return new_tickers


def sort_columns(df, ascending=True):
    return df.reindex(sorted(df.columns, reverse=(ascending==True)), axis=1)