import os
import time
from glob import glob
from typing import Literal

import numpy as np
import pandas as pd
import yfinance as yf

from utils import *


class Database:
    def __init__(self, load_dir: str | None = None):
        root = "DB"
        if load_dir is None:
            version_list = [f for f in os.listdir(root) if os.path.isdir(f"{root}/{f}")]
            folder_name = max(version_list)  # get latest file
            print(f"Loading database from {folder_name}...")
            load_dir = f"DB/{folder_name}"

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

        prices = self.get_prices()
        prices.name = "Price"
        self.value_summary = self.info[["Market Cap", "Sector", "Industry", "Long Business Summary"]].join(prices)

        self.valuation_df_dict = {}
        self.calc_simple_fper_valuation(forward=0)
        self.calc_fpegr_valuation(forward=0)  # default: this year

    def calc_simple_fper_valuation(self, forward: Literal[0, 1] = 1):
        # forward: 0 = this year, 1 = next year
        main_df = self.value_summary[["Sector", "Industry", "Price"]]
        agg_cols = []
        # append forward EPS
        forward_eps = self.get_forward_eps(forward=forward)
        forward_eps = forward_eps.replace(0, np.nan)
        forward_eps.name = f"Forward EPS"
        main_df = main_df.join(forward_eps)
        # calculate forward PER (seeking alpha 검증 결과 음수 PER은 중앙값 계산시 제외하는 것으로 보임)
        forward_per = self.get_prices() / main_df[forward_eps.name]
        forward_per = forward_per.where(forward_per > 0)
        forward_per.name = f"Forward PER"
        agg_cols.append(forward_per.name)
        main_df = main_df.join(forward_per)
        # get sector median
        agg_dict = {key: "median" for key in agg_cols}
        sector_median_df = main_df.groupby("Sector").agg(agg_dict)
        main_df = main_df.merge(
            sector_median_df, left_on="Sector", right_index=True, suffixes=("", "(Sector)"), how="left"
        )
        fair_price = main_df["Forward PER(Sector)"] * main_df["Forward EPS"]
        main_df[f"Fair Price"] = fair_price
        self.valuation_df_dict["fper_valuation"] = main_df  # record output
        # record on summary
        self.value_summary["Simple Forward PER Fair Price"] = fair_price
        return main_df

    def calc_fpegr_valuation(self, forward: Literal[0, 1] = 1):
        # forward: 0 = this year, 1 = next year
        main_df = self.value_summary[["Sector", "Industry", "Price"]]
        agg_cols = []
        # append forward EPS
        forward_eps = self.get_forward_eps(forward=forward)
        forward_eps = forward_eps.replace(0, np.nan)
        forward_eps.name = f"Forward EPS"
        main_df = main_df.join(forward_eps)
        # calculate forward PER (seeking alpha 검증 결과 음수 PER은 중앙값 계산시 제외하는 것으로 보임)
        forward_per = self.get_prices() / main_df[forward_eps.name]
        forward_per = forward_per.where(forward_per > 0)
        forward_per.name = f"Forward PER"
        agg_cols.append(forward_per.name)
        main_df = main_df.join(forward_per)
        # calculate forward PEGR
        eps_growth_pct = self.get_forward_eps_growth_rate(growth_agg="mean") * 100
        positive_eps_growth_pct = eps_growth_pct.where(eps_growth_pct > 0)
        forward_pegr = forward_per / positive_eps_growth_pct
        forward_pegr.name = f"Forward PEGR"
        agg_cols.append(forward_pegr.name)
        main_df = main_df.join(forward_pegr)
        # get sector median
        agg_dict = {key: "median" for key in agg_cols}
        sector_median_df = main_df.groupby("Sector").agg(agg_dict)
        main_df = main_df.merge(
            sector_median_df, left_on="Sector", right_index=True, suffixes=("", "(Sector)"), how="left"
        )
        for col in agg_cols:
            main_df[f"{col}(% Diff.Sector)"] = main_df[col] / main_df[f"{col}(Sector)"] - 1
        fair_per = main_df[f"Forward PEGR(Sector)"] * positive_eps_growth_pct
        fair_price = (fair_per / forward_per) * self.get_prices()
        upside_potential = fair_price / self.get_prices() - 1
        main_df[f"Fair PER"] = fair_per
        main_df[f"Fair Price"] = fair_price
        main_df[f"Upside Potential"] = upside_potential
        self.valuation_df_dict["fpegr_valuation"] = main_df  # record output
        # record on summary
        self.value_summary["Forward PEGR Fair Price"] = fair_price
        return main_df

    def get_ohlcv(self):
        col = self.ohlcv.columns.max()
        return self.ohlcv[col]
    
    def get_prices(self):
        last_ohlcv = self.get_ohlcv()
        return last_ohlcv.xs("Close", level="Type")
    
    def get_volumes(self):
        last_ohlcv = self.get_ohlcv()
        return last_ohlcv.xs("Volume", level="Type")
    
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

    def get_forward_eps_cagr(self, cagr_range = (-1, 1)):
        # cagr_range: (-1, 1) = last year(-1) ~ next year(1)
        start, end = cagr_range
        assert start < 0 <= end
        n = end - start
        eps = self.get_basic_eps(before=abs(start))  # last year
        forward_eps = self.get_forward_eps(forward=end)
        cagr = (forward_eps / eps) ** (1/n) - 1
        return cagr

    def get_forward_eps_growth_rate(self, growth_agg: str = None):
        # growth_agg: "mean", "max", "min"
        eps_growth = self.estimates["EPS YoY Growth"]
        if growth_agg is None:
            return eps_growth
        elif growth_agg.lower() == "mean":
            eps_growth = eps_growth.groupby("Ticker").mean()
        elif growth_agg.lower() == "max":
            eps_growth = eps_growth.groupby("Ticker").max()
        elif growth_agg.lower() == "min":
            eps_growth = eps_growth.groupby("Ticker").min()
        else:
            raise ValueError("growth_agg must be 'mean', 'max' or 'min'.")
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

    def get_pegr_df(self, forward: int | None = None, growth_agg: str = "mean"):
        pegr_series = self.get_forward_pegr(forward, growth_agg).replace([np.inf, -np.inf], np.nan)
        pegr_series.name = "PEGR"
        pegr_df = self.full_df.join(pegr_series)
        return pegr_df


class Indexer:
    def __init__(self, data_path: str | None = None) -> None:
        self._db = Database(data_path)

        self._ticker = None
        self._info = None
        self._price = None
        self._income_statement = None
        self._income_statement_quarter = None
        self._balance_sheet = None
        self._balance_sheet_quarter = None
        self._cash_flow = None
        self._cash_flow_quarter = None
        self._estimates = None

    def __call__(self, ticker: str | None = None):
        self._set_ticker(ticker)

    def _set_ticker(self, ticker: str | None = None):
        if ticker is None:
            if self._ticker is None:
                raise ValueError("Ticker is not set. Please provide a ticker.")
            else:
                return
        else:
            if self._ticker == ticker:
                return
            else:
                self._ticker = ticker.upper()
                self._info = self._db.info.loc[ticker]
                self._price = self._db.price.loc[ticker].sort_index()  # sort by date
                self._income_statement = self._db.income_statement.loc[ticker]
                self._income_statement_quarter = self._db.income_statement_quarter.loc[ticker]
                self._balance_sheet = self._db.balance_sheet.loc[ticker]
                self._balance_sheet_quarter = self._db.balance_sheet_quarter.loc[ticker]
                self._cash_flow = self._db.cash_flow.loc[ticker]
                self._cash_flow_quarter = self._db.cash_flow_quarter.loc[ticker]
                self._estimates = self._db.estimates.loc[ticker]

    def info_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._info
    
    def price_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._price

    def income_statement_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._income_statement
    
    def income_statement_quarter_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._income_statement_quarter
    
    def balance_sheet_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._balance_sheet
    
    def balance_sheet_quarter_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._balance_sheet_quarter
    
    def cash_flow_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._cash_flow
    
    def cash_flow_quarter_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._cash_flow_quarter
    
    def estimates_df(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._estimates
    
    def price(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._price.iloc[-1].item()

    def business_summary(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._info.loc["longBusinessSummary"]
    
    def sector(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._info.loc["sector"]
    
    def industry(self, ticker: str | None = None):
        self._set_ticker(ticker)
        return self._info.loc["industry"]

    def operating_income(self, ticker: str | None = None, before: int = 0):
        self._set_ticker(ticker)
        return self._income_statement.loc["Operating Income"].iloc[before].item()
    
    def quaterly_operating_income(self, ticker: str | None = None, before: int = 0):
        self._set_ticker(ticker)
        return self._income_statement_quarter.loc["Operating Income"].iloc[before].item()

    def forward_per(self, ticker: str | None = None, period: int = 0):
        # period: 0 = this year, 1 = next year
        self._set_ticker(ticker)
        forward_per_df = self._db.get_forward_per(period)
        return forward_per_df.loc[self._ticker].item()
    
    def forward_eps_growth_rate(self, ticker: str | None = None, growth_agg: str = "mean"):
        # growth_agg: "mean", "max", "min"
        self._set_ticker(ticker)
        eps_growth_df = self._db.get_forward_eps_growth_rate(growth_agg)
        return eps_growth_df.loc[self._ticker]
    
    def forward_pegr(self, ticker: str | None = None, period: int = 0, growth_agg: str = "mean"):
        # period: 0 = this year, 1 = next year
        # growth_agg: "mean", "max", "min"
        self._set_ticker(ticker)
        pegr_df = self._db.get_forward_pegr(period, growth_agg)
        return pegr_df.loc[self._ticker]
    
    def sector_pegr(self, ticker: str | None = None, agg: str = "median"):
        # agg: "mean", "median"
        self._set_ticker(ticker)
        sector = self.sector(ticker)
        if agg == "mean":
            pegr_df = self._db.sector_pegr_mean
        elif agg == "median":
            pegr_df = self._db.sector_pegr_median
        else:
            raise ValueError("agg must be 'mean' or 'median'.")
        return pegr_df.loc[sector].item()
    
    def industry_pegr(self, ticker: str | None = None, agg: str = "median"):
        # agg: "mean", "median"
        self._set_ticker(ticker)
        industry = self.industry(ticker)
        if agg == "mean":
            pegr_df = self._db.industry_pegr_mean
        elif agg == "median":
            pegr_df = self._db.industry_pegr_median
        else:
            raise ValueError("agg must be 'mean' or 'median'.")
        return pegr_df.loc[industry].item()