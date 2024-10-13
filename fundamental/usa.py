import time

import numpy as np
import pandas as pd
import yfinance as yf

from utils import *


class YahooFinanceInfoExtractor:
    def __init__(self) -> None:
        self.ticker = None
        self.company = None
        self.history = None

        self.info = None
        self.is_q = None
        self.bs_q = None
        self.cf_q = None

        self.is_y = None
        self.bs_y = None
        self.cf_y = None

        self.is_quarters = None
        self.bs_quarters = None
        self.cf_quarters = None

        self.is_years = None
        self.bs_years = None
        self.cf_years = None

        self.q_updated = False  # 3개(is, bs, cf)의 분기별 리포트의 업데이트 날짜가 동일한지
        self.y_updated = False  # 3개(is, bs, cf)의 연도별 리포트의 업데이트 날짜가 동일한지

    def _download_info(self):
        """
        주식 정보를 다운로드하여 인스턴스 변수에 저장합니다.

        yfinance 라이브러리를 사용하여 주어진 티커에 대한 주식 정보를 다운로드합니다.
        분기별 및 연간 손익 계산서, 재무 상태표, 현금 흐름표를 다운로드하고,
        각 데이터의 날짜 정보를 저장합니다.
        """
        self.company = yf.Ticker(self.ticker)

        self.info = self.company.info
        self.is_q = self.company.quarterly_income_stmt      # 분기 손익계산서, is
        self.bs_q = self.company.quarterly_balance_sheet    # 분기 재무상태표(대차대조표), bs
        self.cf_q = self.company.quarterly_cashflow         # 분기 현금흐름표, cf

        self.is_y = self.company.income_stmt      # 연단위 손익계산서, is
        self.bs_y = self.company.balance_sheet    # 연단위 재무상태표(대차대조표), bs
        self.cf_y = self.company.cashflow         # 연단위 현금흐름표, cf

        self.is_q_dates = self.is_q.columns
        self.bs_q_dates = self.bs_q.columns
        self.cf_q_dates = self.cf_q.columns

        self.is_y_dates = self.is_y.columns
        self.bs_y_dates = self.bs_y.columns
        self.cf_y_dates = self.cf_y.columns

        # self.history = self.company.history()
        for period in ["1mo", "5d", "1d"]:
            history = self.company.history(period=period)
            if not history.empty:
                self.history = history
                break
            time.sleep(3)
        
        self._check_updated()
        return True
    
    def _check_report_ok(self):
        if self.info is None:
            return False
        if self.is_q.empty or self.bs_q.empty or self.cf_q.empty:
            return False
        if self.is_y.empty or self.bs_y.empty or self.cf_y.empty:
            return False
        
        return True

    def _check_updated(self):
        """3개(is, bs, cf)의 연도별 리포트의 업데이트 날짜가 동일한지 확인합니다."""
        if self._check_report_ok():
            if max(self.is_q_dates) == max(self.bs_q_dates) == max(self.cf_q_dates):
                self.q_updated = True
            else:
                self.q_updated = False

            if max(self.is_y_dates) == max(self.bs_y_dates) == max(self.cf_y_dates):
                self.y_updated = True
            else:
                self.y_updated = False
        else:
            self.q_updated = False
            self.y_updated = False
        return

    def setup_ticker(self, ticker):
        assert self.ticker is not None or ticker is not None, "Need to be set the ticker"
        if ticker is not None and ticker != self.ticker:
            self.ticker = ticker
            self._download_info()
    
    @exception_handler
    def get_company_name(self, ticker=None):
        self.setup_ticker(ticker)
        return self.info["longName"]
    
    @exception_handler
    def get_sector(self, ticker=None):
        self.setup_ticker(ticker)
        return self.info["sector"]
    
    @exception_handler
    def get_industry(self, ticker=None):
        self.setup_ticker(ticker)
        return self.info["industry"]
    
    @exception_handler
    def get_market_cap(self, ticker=None):  # 시가총액
        self.setup_ticker(ticker)
        return self.info["marketCap"]
    
    @exception_handler
    def get_capital_stock(self, ticker=None, before=0, is_quarter=True):  # 자본금
        self.setup_ticker(ticker)
        if is_quarter:
            report = self.bs_q
            date = self.bs_q_dates[before]
        else:
            report = self.bs_y
            date = self.bs_y_dates[before]
        
        if "Capital Stock" in report.index:
            values = report.loc["Capital Stock", date]
        else:
            values = None
        return values

    @exception_handler
    def get_stockholders_equity(self, ticker=None, before=0, is_quarter=True):  # 자기자본
        self.setup_ticker(ticker)
        if is_quarter:
            report = self.bs_q
            date = self.bs_q_dates[before]
        else:
            report = self.bs_y
            date = self.bs_y_dates[before]
        
        if "Stockholders Equity" in report.index:
            values = report.loc["Stockholders Equity", date]
        else:
            values = None
        return values
    
    @exception_handler
    def get_net_income(self, ticker=None, before=0, is_quarter=True):   # 순이익
        self.setup_ticker(ticker)
        if is_quarter:
            report = self.is_q
            date = self.is_q_dates[before]
        else:
            report = self.is_y
            date = self.is_y_dates[before]
        
        if "Net Income" in report.index:
            values = report.loc["Net Income", date]
        else:
            values = None
        return values

    @exception_handler
    def get_operating_income(self, ticker=None, before=0, is_quarter=True):     # 영업이익
        self.setup_ticker(ticker)
        if is_quarter:
            report = self.is_q
            date = self.is_q_dates[before]
        else:
            report = self.is_y
            date = self.is_y_dates[before]
        
        if "Operating Income" in report.index:
            values = report.loc["Operating Income", date]
        else:
            values = None
        return values

    @exception_handler
    def get_operating_revenue(self, ticker=None, before=0, is_quarter=True):    # 영업수익(총매출)
        self.setup_ticker(ticker)
        if is_quarter:
            report = self.is_q
            date = self.is_q_dates[before]
        else:
            report = self.is_y
            date = self.is_y_dates[before]
        
        if "Total Revenue" in report.index:
            values = report.loc["Total Revenue", date]
        else:
            values = None
        return values

    @exception_handler
    def get_operating_cashflow(self, ticker=None, before=0, is_quarter=True):  # 영업활동현금흐름
        self.setup_ticker(ticker)
        if is_quarter:
            report = self.cf_q
            date = self.cf_q_dates[before]
        else:
            report = self.cf_y
            date = self.cf_y_dates[before]
        
        if "Operating Cash Flow" in report.index:
            values = report.loc["Operating Cash Flow", date]
        else:
            values = None
        return values
    
    @exception_handler
    def get_price(self, ticker=None):
        self.setup_ticker(ticker)
        return self.history.iloc[-1]["Close"].item()
    
    @exception_handler
    def get_num_shares(self, ticker=None, before=0):
        self.setup_ticker(ticker)
        if self.bs_q is not None and "Share Issued" in self.bs_q.index:
            return self.bs_q.loc["Share Issued", self.bs_q_dates[before]]
        else:
            start = get_today(before_day=90*(before+1), to_str=True)
            return self.company.get_shares_full(start=start).iloc[-1]   # TODO: closest date row 가져오는 코드 구현 필요
    
    @exception_handler
    def get_eps(self, ticker=None, before=0):     # 당기순이익 / 유통주식수
        self.setup_ticker(ticker)
        return nandiv(self.get_net_income(before=before), self.get_num_shares(before=before))

    @exception_handler
    def get_bps(self, ticker=None):     # 자기자본 / 유통주식수
        self.setup_ticker(ticker)
        return nandiv(self.get_stockholders_equity(), self.get_num_shares())

    @exception_handler
    def get_sps(self, ticker=None):     # 매출액 / 유통주식수
        self.setup_ticker(ticker)
        return nandiv(self.get_operating_revenue(), self.get_num_shares())

    @exception_handler
    def get_roe(self, ticker=None):     # 당기순이익 / 자기자본
        self.setup_ticker(ticker)
        return nandiv(self.get_net_income(), self.get_stockholders_equity())
    
    @exception_handler
    def get_per(self, ticker=None):     # 주가 / EPS
        self.setup_ticker(ticker)
        return nandiv(self.get_price(), (self.get_eps()))
    
    @exception_handler
    def get_pbr(self, ticker=None):     # 주가 / BPS
        self.setup_ticker(ticker)
        return nandiv(self.get_price(), (self.get_bps()))
    
    @exception_handler
    def get_psr(self, ticker=None):     # 주가 / SPS
        self.setup_ticker(ticker)
        return nandiv(self.get_price(), (self.get_sps()))
    
    @exception_handler
    def get_pegr(self, ticker=None):    # PER / EPS 증가율
        self.setup_ticker(ticker)
        eps_growth_rate_list = []
        for now in range(4):
            before = now + 1
            now_eps, before_eps = self.get_eps(before=now), self.get_eps(before=before)
            eps_growth_rate_list.append(calc_growth_rate(now_eps, before_eps))
        if all(eps_growth_rate_list):   # eps_growth_rate_list의 모든 값이 None이 아닌 실수일 때,
            eps_growth_rate = np.mean(eps_growth_rate_list).item()
        else:
            eps_growth_rate = None
        return nandiv(self.get_per(), eps_growth_rate)