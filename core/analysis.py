import os
import time
from glob import glob
import datetime
from typing import Literal

import numpy as np
from scipy.stats import gmean
import pandas as pd

from utils import *


class Database:
    def __init__(self, load_dir: str | None = None):
        nation = "usa" # usa, kor
        root = f"DB/{nation}"
        if load_dir is None:
            version_list = [f for f in os.listdir(root) if os.path.isdir(f"{root}/{f}")]
            self.version = max(version_list)  # get latest file
            print(f"Loading database from {self.version}...")
            load_dir = f"{root}/{self.version}"
        else:
            self.version = os.path.basename(load_dir)

        self._dt_version = datetime.datetime.strptime(self.version, "%y%m%d")
        self._dt_today = datetime.datetime.today()

        # check version format is like yymmdd
        assert len(self.version) == 6 and self.version.isdigit(), "Version format must be yymmdd."

        self.info = pd.read_parquet(f"{load_dir}/info.parquet")
        self.ohlcv = pd.read_parquet(f"{load_dir}/ohlcv.parquet")
        self.income_statement = pd.read_parquet(f"{load_dir}/income_statement.parquet")
        self.income_statement_quarter = pd.read_parquet(f"{load_dir}/income_statement_quarter.parquet")
        self.balance_sheet = pd.read_parquet(f"{load_dir}/balance_sheet.parquet")
        self.balance_sheet_quarter = pd.read_parquet(f"{load_dir}/balance_sheet_quarter.parquet")
        self.cash_flow = pd.read_parquet(f"{load_dir}/cash_flow.parquet")
        self.cash_flow_quarter = pd.read_parquet(f"{load_dir}/cash_flow_quarter.parquet")
        self.estimates = pd.read_parquet(f"{load_dir}/estimates.parquet").sort_index()

        self.sector_list = self.info["Sector"].replace("", None).dropna().unique().tolist()
        self.industry_list = self.info["Industry"].replace("", None).dropna().unique().tolist()

        self.stocks_df, self.sectors_df, self.industries_df = self._make_stocks_and_sectors()
        self.valuation_df = self.stocks_df[["Sector", "Industry", "Long Business Summary", "Market Cap", "Price"]].copy()

        self.calc_simple_fper_valuation(forward=0)
        self.calc_fpegr_valuation(forward=0)  # default: this year

    def _make_stocks_and_sectors(self):
        stocks_df = self.info.copy()
        stocks_df["Market Cap"] = stocks_df["Market Cap"].replace("", np.nan).astype(float)
        stocks_df["Price"] = self.get_prices()
        stocks_df["Volume"] = self.get_volumes()
        stocks_df["Trading Value"] = self.get_trading_values()

        # add forward eps
        stocks_df["Forward EPS (This Year)"] = self.get_forward_eps(forward=0)
        stocks_df["Forward EPS (Next Year)"] = self.get_forward_eps(forward=1)

        # add forward eps growth rate
        stocks_df["Forward EPS Growth (This Year)"] = self.get_forward_eps_growth_rate(return_type="this")
        stocks_df["Forward EPS Growth (Next Year)"] = self.get_forward_eps_growth_rate(return_type="next")
        stocks_df["Forward EPS Growth (CAGR)"] = self.get_forward_eps_growth_rate(return_type="cagr")

        agg_cols = []
        # add forward PER (seeking alpha 검증 결과 음수 PER은 Sector 중앙값 계산시 제외하는 것으로 보임)
        positive_this_eps = stocks_df["Forward EPS (This Year)"].where(stocks_df["Forward EPS (This Year)"] > 0)
        this_forward_per = stocks_df["Price"] / positive_this_eps
        this_forward_per = this_forward_per.where(this_forward_per > 0)  # Seeking Alpha does not include negative PER
        this_forward_per.name = "Forward PER (This Year)"
        stocks_df = stocks_df.join(this_forward_per)
        positive_next_eps = stocks_df["Forward EPS (Next Year)"].where(stocks_df["Forward EPS (Next Year)"] > 0)
        next_forward_per = stocks_df["Price"] / positive_next_eps
        next_forward_per = next_forward_per.where(next_forward_per > 0)  # Seeking Alpha does not include negative PER
        next_forward_per.name = "Forward PER (Next Year)"
        stocks_df = stocks_df.join(next_forward_per)
        agg_cols += [this_forward_per.name, next_forward_per.name]

        # add forward PEGR
        forward_eps_growth = stocks_df["Forward EPS Growth (CAGR)"].replace("", np.nan).astype(float)
        positive_eps_growth_pct = forward_eps_growth.where(forward_eps_growth > 0) * 100
        this_forward_pegr = this_forward_per / positive_eps_growth_pct
        this_forward_pegr.name = "Forward PEGR (This Year)"
        stocks_df = stocks_df.join(this_forward_pegr)
        next_forward_pegr = next_forward_per / positive_eps_growth_pct
        next_forward_pegr.name = "Forward PEGR (Next Year)"
        stocks_df = stocks_df.join(next_forward_pegr)
        agg_cols += [this_forward_pegr.name, next_forward_pegr.name]

        # add sector and industry median
        agg_dict = {key: "median" for key in agg_cols}
        sectors_df = stocks_df.groupby("Sector").agg(agg_dict).add_prefix("Sector ")
        industries_df = stocks_df.groupby("Industry").agg(agg_dict).add_prefix("Industry ")

        return stocks_df, sectors_df, industries_df
    
    def calc_simple_fper_valuation(self, forward: Literal[0, 1] = 1):
        # forward: 0 = this year, 1 = next year
        period = "(This Year)" if forward == 0 else "(Next Year)"
        if f"Forward PER Fair Price {period}" in self.valuation_df.columns:
            return self.valuation_df[f"Forward PER Fair Price {period}"]
        else:
            forward_eps = self.stocks_df[f"Forward EPS {period}"]
            sector_forward_per = self.stocks_df.join(self.sectors_df, on="Sector")[f"Sector Forward PER {period}"]
            fair_price = sector_forward_per * forward_eps
            fair_price.name = f"Forward PER Fair Price {period}"
            self.valuation_df[f"Forward PER Fair Price {period}"] = fair_price
            return fair_price

    def calc_fpegr_valuation(self, forward: Literal[0, 1] = 1):
        # forward: 0 = this year, 1 = next year
        period = "(This Year)" if forward == 0 else "(Next Year)"
        if f"Forward PEGR Fair Price {period}" in self.valuation_df.columns:
            return self.valuation_df[f"Forward PEGR Fair Price {period}"]
        else:
            forward_eps = self.stocks_df[f"Forward EPS {period}"]
            sector_forward_pegr = self.stocks_df.join(self.sectors_df, on="Sector")[f"Sector Forward PEGR {period}"]
            eps_growth_pct = self.stocks_df["Forward EPS Growth (CAGR)"] * 100
            positive_eps_growth_pct = eps_growth_pct.where(eps_growth_pct > 0)
            fair_per = sector_forward_pegr * positive_eps_growth_pct
            fair_price = (fair_per / forward_eps) * self.stocks_df["Price"]
            fair_price.name = f"Forward PEGR Fair Price {period}"
            self.valuation_df[f"Forward PEGR Fair Price {period}"] = fair_price
            return fair_price

    def get_prices(self, period: Literal['1d', '5d'] = '1d'):
        if period == '1d':
            col = self.ohlcv.columns[-1]
        elif period == '5d':
            col = self.ohlcv.columns[-5:]
        ohlcv = self.ohlcv[col]
        return ohlcv.xs("Close", level="Type")

    def get_volumes(self, period: Literal['1d', '5d'] = '1d'):
        if period == '1d':
            col = self.ohlcv.columns[-1]
        elif period == '5d':
            col = self.ohlcv.columns[-5:]
        ohlcv = self.ohlcv[col]
        return ohlcv.xs("Volume", level="Type")

    def get_trading_values(self, period: Literal['1d', '5d'] = '1d'):
        return self.get_prices(period) * self.get_volumes(period)

    def get_basic_eps(self, before: int = None):
        # before: 1 = last year, 2 = 2 year ago ...
        income_statement = self.income_statement.sort_index(ascending=[True, False])
        if isinstance(before, int):
            assert 0 < before < 5
            income_statement = income_statement.groupby("Ticker").nth(before - 1).droplevel(1)
        eps = income_statement["Basic EPS"]
        return eps
    
    def get_forward_eps(self, forward: int = None):
        # forward: 0 = this year, 1 = next year
        forward_eps = self.estimates["EPS Estimate"]
        if isinstance(forward, int):
            forward_eps = forward_eps.groupby("Ticker").nth(forward).droplevel(1)
        return forward_eps

    def get_forward_eps_growth_rate(self, return_type: str = None):
        # return_type: 0(this), 1(next), "mean", "max", "min", "cagr", "gmean"
        eps_growth = self.estimates["EPS YoY Growth"]
        if return_type is None:
            return eps_growth
        elif return_type == "this" or return_type == 0:
            eps_growth = eps_growth.groupby("Ticker").nth(0).droplevel(1)
        elif return_type == "next" or return_type == 1:
            eps_growth = eps_growth.groupby("Ticker").nth(1).droplevel(1)
        elif return_type.lower() == "mean":
            eps_growth = eps_growth.groupby("Ticker").mean()
        elif return_type.lower() == "max":
            eps_growth = eps_growth.groupby("Ticker").max()
        elif return_type.lower() == "min":
            eps_growth = eps_growth.groupby("Ticker").min()
        elif return_type.lower() == "cagr" or return_type.lower() == "gmean":
            eps_growth = eps_growth.groupby("Ticker").apply(lambda x: gmean(x + 1) - 1)
        else:
            raise ValueError("growth_agg must be 'mean', 'cagr', 'max' or 'min'.")
        return eps_growth
    
    def get_tickers_from_sector(self, sector: str | None = None):
        if sector is None:
            return self.info.index.tolist()
        else:
            if sector not in self.info["Sector"].unique():
                raise ValueError(f"Sector '{sector}' not found in the database.")
            return self.info[self.info["sector"] == sector].index.tolist()
        
    def get_tickers_from_industry(self, industry: str | None = None):
        if industry is None:
            return self.info.index.tolist()
        else:
            if industry not in self.info["industry"].unique():
                raise ValueError(f"Industry '{industry}' not found in the database.")
            return self.info[self.info["industry"] == industry].index.tolist()

    def get_same_sector_tickers(self, ticker: str):
        info = self.info.loc[ticker]
        sector = info.loc["Sector"]
        return self.get_tickers_from_sector(sector)
    
    def get_same_industry_tickers(self, ticker: str):
        info = self.info.loc[ticker]
        industry = info.loc["Industry"]
        return self.get_tickers_from_industry(industry)


# class Analyzer:
#     def __init__(self, data_path: str | None = None) -> None:
#         self._db = Database(data_path)

#         self._ticker = None
#         self._info = None
#         self._price = None
#         self._income_statement = None
#         self._income_statement_quarter = None
#         self._balance_sheet = None
#         self._balance_sheet_quarter = None
#         self._cash_flow = None
#         self._cash_flow_quarter = None
#         self._estimates = None

#     def __call__(self, ticker: str | None = None):
#         self._set_ticker(ticker)

#     def _set_ticker(self, ticker: str | None = None):
#         if ticker is None:
#             if self._ticker is None:
#                 raise ValueError("Ticker is not set. Please provide a ticker.")
#             else:
#                 return
#         else:
#             if self._ticker == ticker:
#                 return
#             else:
#                 self._ticker = ticker.upper()
#                 self._info = self._db.info.loc[ticker]
#                 self._price = self._db.price.loc[ticker].sort_index()  # sort by date
#                 self._income_statement = self._db.income_statement.loc[ticker]
#                 self._income_statement_quarter = self._db.income_statement_quarter.loc[ticker]
#                 self._balance_sheet = self._db.balance_sheet.loc[ticker]
#                 self._balance_sheet_quarter = self._db.balance_sheet_quarter.loc[ticker]
#                 self._cash_flow = self._db.cash_flow.loc[ticker]
#                 self._cash_flow_quarter = self._db.cash_flow_quarter.loc[ticker]
#                 self._estimates = self._db.estimates.loc[ticker]

#     def info_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._info
    
#     def price_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._price

#     def income_statement_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._income_statement
    
#     def income_statement_quarter_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._income_statement_quarter
    
#     def balance_sheet_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._balance_sheet
    
#     def balance_sheet_quarter_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._balance_sheet_quarter
    
#     def cash_flow_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._cash_flow
    
#     def cash_flow_quarter_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._cash_flow_quarter
    
#     def estimates_df(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._estimates
    
#     def price(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._price.iloc[-1].item()

#     def business_summary(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._info.loc["longBusinessSummary"]
    
#     def sector(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._info.loc["sector"]
    
#     def industry(self, ticker: str | None = None):
#         self._set_ticker(ticker)
#         return self._info.loc["industry"]

#     def operating_income(self, ticker: str | None = None, before: int = 0):
#         self._set_ticker(ticker)
#         return self._income_statement.loc["Operating Income"].iloc[before].item()
    
#     def quaterly_operating_income(self, ticker: str | None = None, before: int = 0):
#         self._set_ticker(ticker)
#         return self._income_statement_quarter.loc["Operating Income"].iloc[before].item()

#     def forward_per(self, ticker: str | None = None, period: int = 0):
#         # period: 0 = this year, 1 = next year
#         self._set_ticker(ticker)
#         forward_per_df = self._db.get_forward_per(period)
#         return forward_per_df.loc[self._ticker].item()
    
#     def forward_eps_growth_rate(self, ticker: str | None = None, growth_agg: str = "mean"):
#         # growth_agg: "mean", "max", "min"
#         self._set_ticker(ticker)
#         eps_growth_df = self._db.get_forward_eps_growth_rate(growth_agg)
#         return eps_growth_df.loc[self._ticker]
    
#     def forward_pegr(self, ticker: str | None = None, period: int = 0, growth_agg: str = "mean"):
#         # period: 0 = this year, 1 = next year
#         # growth_agg: "mean", "max", "min"
#         self._set_ticker(ticker)
#         pegr_df = self._db.get_forward_pegr(period, growth_agg)
#         return pegr_df.loc[self._ticker]
    
#     def sector_pegr(self, ticker: str | None = None, agg: str = "median"):
#         # agg: "mean", "median"
#         self._set_ticker(ticker)
#         sector = self.sector(ticker)
#         if agg == "mean":
#             pegr_df = self._db.sector_pegr_mean
#         elif agg == "median":
#             pegr_df = self._db.sector_pegr_median
#         else:
#             raise ValueError("agg must be 'mean' or 'median'.")
#         return pegr_df.loc[sector].item()
    
#     def industry_pegr(self, ticker: str | None = None, agg: str = "median"):
#         # agg: "mean", "median"
#         self._set_ticker(ticker)
#         industry = self.industry(ticker)
#         if agg == "mean":
#             pegr_df = self._db.industry_pegr_mean
#         elif agg == "median":
#             pegr_df = self._db.industry_pegr_median
#         else:
#             raise ValueError("agg must be 'mean' or 'median'.")
#         return pegr_df.loc[industry].item()