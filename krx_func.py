import re
import datetime

from pytimekr import pytimekr
import pandas as pd
import pykrx
import dart_fss

from utils import *

OPENDART_APIKEY = "f15d5e316baebf5fa004412c2feefb4a8dd65745"
dart_fss.set_api_key(api_key=OPENDART_APIKEY)


def get_fs_df(fs, fs_type, row_name):
    assert fs_type in ["bs", "is", "cf"]    # [재무상태표, 손익계산서, 현금흐름표]
    df = fs[fs_type].copy()     
    df.columns = df.columns.droplevel(level=1)
    if fs_type == "bs":
        df = df[df.iloc[:, 1] == row_name].filter(regex="^[0-9]+$")
    else:
        df = df[df.iloc[:, 1] == row_name].filter(regex="^[0-9]+-[0-9]+$")
    assert not df.empty

    return df


def calc_quarter_value(df: pd.DataFrame, year: int):
    df = df.filter(regex=f"{year}.")

    value_1st = None
    value_2nd = None
    value_3rd = None
    value_4th = None

    if f"{year}0101-{year}0331" in df.columns:
        value_1st = df[f"{year}0101-{year}0331"].item()

    if f"{year}0101-{year}0630" in df.columns:
        value_2nd = df[f"{year}0101-{year}0630"].item() - df[f"{year}0101-{year}0331"].item()

    if f"{year}0101-{year}0930" in df.columns:
        value_3rd = df[f"{year}0101-{year}0930"].item() - df[f"{year}0101-{year}0630"].item()

    if f"{year}0101-{year}1231" in df.columns:
        value_4th = df[f"{year}0101-{year}1231"].item() - df[f"{year}0101-{year}0930"].item()

    return value_1st, value_2nd, value_3rd, value_4th


def get_value(df: pd.DataFrame):
    year = datetime.date.today().year
    while True:
        this_year_value = calc_quarter_value(df, year)

        reported_quarter = len([v for v in this_year_value if v is not None])
        if reported_quarter == 0:
            year -= 1
        else:
            break

    return this_year_value[reported_quarter - 1]


def get_two_values(df: pd.DataFrame, compare_with: str):
    year = datetime.date.today().year
    while True:
        last_yaer = year - 1
        this_year_value = calc_quarter_value(df, year)
        last_year_value = calc_quarter_value(df, last_yaer)

        reported_quarter = len([v for v in this_year_value if v is not None])
        if reported_quarter == 0:
            year -= 1
        else:
            break
    
    if compare_with == "last_quarter":   # 직전 분기
        if reported_quarter == 1:    # 1분기 값만 있을 경우
            this, prev = this_year_value[0], last_year_value[3]
        else:
            this, prev = this_year_value[reported_quarter - 1], this_year_value[reported_quarter - 2]
    elif compare_with == "last_year":    # 작년 동분기
        this, prev = this_year_value[reported_quarter - 1], last_year_value[reported_quarter - 1]
    else:
        raise NotImplementedError

    return this, prev


class DartInfoExtractor:
    def __init__(self):
        df = pd.DataFrame(dart_fss.api.filings.get_corp_code())
        self.corp_code_list = df[df['stock_code'].notnull()]     # 상장회사 코드 리스트

        self.corp_list = dart_fss.get_corp_list()
        self.fs = {}                # key: stock code
        self.fs_seperate = {}       # key: stock code
        self.ohlcv = {}             # key: stock code
        self.fundamental = {}       # key: date
        self.total_shares = {}    # key: stock code

    def get_corp_code(self, corp: str):
        p = re.compile("^[0-9]+$")
        if p.match(corp):   # 만약 stock code로 입력되면
            corp_code = self.corp_code_list[self.corp_code_list['stock_code'] == corp].iloc[0,0]
        else:   # 그 외는 name으로 검색
            corp_code = self.corp_code_list[self.corp_code_list['corp_name'] == corp].iloc[0,0]
        return corp_code
    
    def download_fs(self, stock_code):
        company = self.corp_list.find_by_stock_code(stock_code)
        if stock_code in self.fs:
            fs = self.fs[stock_code]
        else:
            begin = datetime.date.today() - datetime.timedelta(days=730)    # 오늘로부터 2년 전까지의 데이터 로드
            fs = company.extract_fs(bgn_de=begin.strftime("%Y%m%d"), report_tp="quarter")   # 연결재무제표
            self.fs[stock_code] = fs

    def download_fs_seperate(self, stock_code):
        company = self.corp_list.find_by_stock_code(stock_code)
        if stock_code in self.fs_seperate:
            fs = self.fs_seperate[stock_code]
        else:
            begin = datetime.date.today() - datetime.timedelta(days=730)    # 오늘로부터 2년 전까지의 데이터 로드
            fs = company.extract_fs(bgn_de=begin.strftime("%Y%m%d"), report_tp="quarter", separate=True)    # 개별재무제표
            self.fs_seperate[stock_code] = fs

    def download_fundamental(self, date, market="ALL"):
        if date not in self.fundamental:
            self.fundamental[date] = pykrx.stock.get_market_fundamental(date, market=market)
    
    def download_ohlcv(self, stock_code):
        today = datetime.date.today()
        date = calc_last_business_day(today.year, today.month, today.day).strftime("%Y%m%d")
        if stock_code not in self.ohlcv:
            self.ohlcv[stock_code] = pykrx.stock.get_market_ohlcv(date, date, stock_code)

    def net_profit_growth(self, stock_code: str, compare: str):     # 순이익 성장률
        self.download_fs(stock_code)
        fs = self.fs[stock_code]

        df = get_fs_df(fs, "is", "지배기업의 소유주에게 귀속되는 당기순이익(손실)")
        this, prev = get_two_values(df, compare)
        return (this - prev) / (prev + 10e-10) * 100

    def operating_profit_growth(self, stock_code: str, compare: str):   # 영업이익 성장률
        self.download_fs(stock_code)
        fs = self.fs[stock_code]

        df = get_fs_df(fs, "is", "영업이익")
        this, prev = get_two_values(df, compare)
        return (this - prev) / (prev + 10e-10) * 100
    
    def revenue_growth(self, stock_code: str, compare: str):    # 매출액 성장률
        self.download_fs(stock_code)
        fs = self.fs[stock_code]

        df = get_fs_df(fs, "is", "영업수익")
        this, prev = get_two_values(df, compare)
        return (this - prev) / (prev + 10e-10) * 100
    
    def operating_cash_flow(self, stock_code: str, compare: str):    # 영업활동현금흐름
        self.download_fs(stock_code)
        fs = self.fs[stock_code]

        df = get_fs_df(fs, "cf", "영업활동현금흐름")
        this, prev = get_two_values(df, compare)
        return (this - prev) / (prev + 10e-10) * 100
    
    def price(self, stock_code):
        self.download_ohlcv(stock_code)
        data = self.ohlcv[stock_code]
        return data["종가"].item()
    
    def shares(self, stock_code):
        corp_code = self.get_corp_code(stock_code)
        year = datetime.date.today().year - 1
        if not self.total_shares:
            shares_df = pd.DataFrame(
                dart_fss.api.info.stock_totqy_sttus(corp_code, bsns_year=str(year), reprt_code="11011")["list"]
            )
            total_shares = shares_df[shares_df["se"] == "합계"]["istc_totqy"].item()
            self.total_shares = int(total_shares.replace(",", ""))
        return self.total_shares
    
    def earning(self, stock_code, seperate):
        if seperate:
            self.download_fs_seperate(stock_code)
            df = get_fs_df(self.fs_seperate[stock_code], "is", "당기순이익(손실)")
        else:
            self.download_fs(stock_code)
            df = get_fs_df(self.fs[stock_code], "is", "지배기업의 소유주에게 귀속되는 당기순이익(손실)")

        earning = get_value(df)
        return earning
    
    def sales(self, stock_code, seperate):
        if seperate:
            self.download_fs_seperate(stock_code)
            fs = self.fs_seperate[stock_code]
        else:
            self.download_fs(stock_code)
            fs = self.fs[stock_code]

        df = get_fs_df(fs, "is", "영업수익")
        sales = get_value(df)
        return sales
    
    def equity(self, stock_code, seperate):
        if seperate:
            self.download_fs_seperate(stock_code)
            fs = self.fs_seperate[stock_code]
        else:
            self.download_fs(stock_code)
            fs = self.fs[stock_code]
        
        df = get_fs_df(fs, "bs", "자본총계")
        equity = df.iloc[:, 0].item()   # bs는 가장 왼쪽에 있는 컬럼이 최신
        return equity

    def ROE(self, stock_code):
        equity = self.equity(stock_code, seperate=True)
        earning = self.earning(stock_code, seperate=True)
        return (earning / equity) * 100

    def EPS(self, stock_code):
        today = datetime.date.today()
        date = calc_last_business_day(today.year, today.month, today.day).strftime("%Y%m%d")

        self.download_fundamental(date)
        fundamental = self.fundamental[date]

        data = fundamental[fundamental.index == stock_code]
        return data["EPS"].item()

    def SPS(self, stock_code):
        sales = self.sales(stock_code)
        shares = self.shares(stock_code)
        return sales / shares
    
    def PER(self, stock_code):
        today = datetime.date.today()
        date = calc_last_business_day(today.year, today.month, today.day).strftime("%Y%m%d")

        self.download_fundamental(date)
        fundamental = self.fundamental[date]

        data = fundamental[fundamental.index == stock_code]
        return data["PER"].item()

    def PBR(self, stock_code):
        today = datetime.date.today()
        date = calc_last_business_day(today.year, today.month, today.day).strftime("%Y%m%d")

        self.download_fundamental(date)
        fundamental = self.fundamental[date]

        data = fundamental[fundamental.index == stock_code]
        return data["PBR"].item()
    
    def PEGR(self, stock_code):
        pass

    def PSR(self, stock_code):
        price = self.price(stock_code)
        sps = self.SPS(stock_code)
        return price / sps