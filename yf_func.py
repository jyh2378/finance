from concurrent.futures import ProcessPoolExecutor
import random
import time
import traceback

import numpy as np
import pandas as pd
import yfinance as yf

from utils import *

FMP_API_KEY = "VZeQS1DlDmi2V7LWKjvWHyVH80OVlSzC"


class YahooFinanceInfoExtractor:
    def __init__(self) -> None:
        self.ticker = None
        self.company = None
        self.info = None
        self.income_statement = None
        self.balance_sheet = None
        self.cashflow = None
        self.point_time = None
        self.ttm = None
    
    def download_fs(self, ticker):
        success = False
        self.ticker = ticker
        self.company = yf.Ticker(self.ticker)
        self.info = self.company.info
        self.income_statement = self.company.quarterly_income_stmt    # 손익계산서, is
        self.balance_sheet = self.company.quarterly_balance_sheet     # 재무상태표(대차대조표), bs
        self.cashflow = self.company.quarterly_cashflow               # 현금흐름표, cf
        if self.check_report_ok():
            self.make_date_cols()
            success = True
        return success
    
    def check_report_ok(self):
        if min(len(self.income_statement.columns), len(self.balance_sheet.columns), len(self.cashflow.columns)) == 0:
            return False
        recent = max(self.income_statement.columns.union(self.balance_sheet.columns).union(self.cashflow.columns)).date()
        recent_quarters = get_recent_5q_dates(recent)
        for d in recent_quarters:
            d = pd.Timestamp(d)
            if not d in self.income_statement.columns:
                return False
        return True

    def make_date_cols(self):
        if self.income_statement.columns[0] > self.cashflow.columns[0]:
            recent = self.income_statement.columns[0]
            self.cashflow[recent] = self.cashflow[self.cashflow.columns[0]]
        self.point_time = self.income_statement.columns[[0, 1, 4]]    # this, quarter(QoQ), quarter(YoY)
        self.ttm = self.income_statement.columns[:5]                  # this, ..., quarter on last year(YoY)

    def get_company_name(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "shortName" in self.info:
            return self.info["shortName"]
        else:
            return "unknown"

    def get_net_income(self, ticker, ttm=False):   # 당기 순이익
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "Net Income" in self.income_statement.index:
            col = self.ttm if ttm else self.point_time
            values = self.income_statement.loc["Net Income", col]
        else:
            values = (None, None, None, None, None) if ttm else (None, None, None)
        return values

    def get_operating_income(self, ticker, ttm=False):     # 영업이익
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "Operating Income" in self.income_statement.index:
            col = self.ttm if ttm else self.point_time
            values = self.income_statement.loc["Operating Income", col]
        else:
            values = (None, None, None, None, None) if ttm else (None, None, None)
        return values

    def get_total_revenue(self, ticker, ttm=False):        # 총매출
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "Total Revenue" in self.income_statement.index:
            col = self.ttm if ttm else self.point_time
            values = self.income_statement.loc["Total Revenue", col]
        else:
            values = (None, None, None, None, None) if ttm else (None, None, None)
        return values
    
    def get_operating_cashflow(self, ticker, ttm=False):   # 영업활동현금흐름
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "Operating Cash Flow" in self.cashflow.index:
            col = self.ttm if ttm else self.point_time
            values = self.cashflow.loc["Operating Cash Flow", col]
        else:
            values = (None, None, None, None, None) if ttm else (None, None, None)
        return values
    
    def get_price(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        return self.company.history(period="1d")["Close"].item()

    def get_num_shares(self, ticker, ttm=False):
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "Share Issued" in self.balance_sheet.index and self.ttm[0] in self.balance_sheet.columns:
            col = self.ttm if ttm else self.ttm[0]
            values = self.balance_sheet.loc["Share Issued", col]
        else:
            if ttm:
                start = get_today(before_day=500).strftime("%Y-%m-%d")
                shares_history = self.company.get_shares_full(start=start)
                shares_history = shares_history[~shares_history.index.duplicated(keep="last")]
                shares_history.index = shares_history.index.date
                values = shares_history.iloc[shares_history.index.get_indexer(self.ttm.date, method="backfill")]
            else:
                start = get_today(before_day=90).strftime("%Y-%m-%d")
                values = self.company.get_shares_full(start=start).iloc[-1]
        return values

    def get_equity(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "Stockholders Equity" in self.balance_sheet.index:
            out = self.balance_sheet[self.balance_sheet.columns[0]]["Stockholders Equity"]
        else:
            out = None
        return out
    
    def EPS(self, ticker, ttm=False):
        if self.ticker != ticker:
            self.download_fs(ticker)
        if ttm:
            earning = self.get_net_income(ticker, ttm)
            shares = self.get_num_shares(ticker, ttm) + 10e-10
            eps = earning.values / shares.values
        else:
            earning = self.get_net_income(ticker).iloc[0]
            shares = self.get_num_shares(ticker)
            eps = nandiv(earning, shares)
        return eps
    
    def BPS(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "bookValue" in self.info:
            bps = self.info["bookValue"]
        else:
            bps = nandiv(self.get_equity(ticker), self.get_num_shares(ticker))
        return bps

    def SPS(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        _, _, revenue = self.get_total_revenue(ticker)
        shares = self.get_num_shares(ticker)
        sps = nandiv(revenue, shares)
        return sps

    def ROE(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        _, _, net_income = self.get_net_income(ticker)
        equity = self.get_equity(ticker)
        roe = nandiv(net_income, equity)
        return roe
    
    def PBR(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        # if "priceToBook" in self.info:
        #     pbr = self.info["priceToBook"]
        pbr = nandiv(self.get_price(ticker), self.BPS(ticker))
        return pbr
    
    def PSR(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        psr = nandiv(self.get_price(ticker), self.SPS(ticker))
        return psr
    
    def PER(self, ticker):
        if self.ticker != ticker:
            self.download_fs(ticker)
        if "marketCap" in self.info:
            per = self.info["marketCap"] / self.get_net_income(ticker).iloc[0]
        else:
            per = self.get_price(ticker) / self.EPS(ticker)
        return per
    
    def PEGR(self, ticker):     # https://www.fe.training/free-resources/valuation/price-earnings-to-growth-peg-ratio
        if self.ticker != ticker:
            self.download_fs(ticker)
        # if "pegRatio" in self.info:
        #     out = self.info["pegRatio"]
        eps_ttm = self.EPS(ticker, ttm=True)
        mean_eps_growth = np.mean([calc_growth_rate(v, eps_ttm[i+1]) for i, v in enumerate(eps_ttm[:-1])])
        return self.PER(ticker) / mean_eps_growth
