import logging
import os
import time
import random
import traceback
from typing import get_args, Literal
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
from tqdm import tqdm
import yfinance as yf

from utils import *

YFDataType = Literal[
    "info",
    "income_statement",
    "income_statement_quarter",
    "balance_sheet",
    "balance_sheet_quarter",
    "cash_flow",
    "cash_flow_quarter",
    "estimates",
    "ohlcv"
]

def _camel_to_title_case(text: str) -> str:
    acronyms = {"PE": "Pe"}
    for word, upper_word in acronyms.items():
        text = text.replace(word, upper_word)
    spaced = re.sub(r'([A-Z])', r' \1', text)
    title_case = spaced.strip().title()
    return title_case


def _postprocess_info(info: dict, ticker: str) -> pd.DataFrame:
    key_list = [
        "longName", "sector", "industry", "longBusinessSummary", "quoteType", "lastFiscalYearEnd", "sharesOutstanding",
        "marketCap", "enterpriseValue",
        "trailingPE", "forwardPE", "trailingEps", "forwardEps", "trailingPegRatio", "beta",
    ]
    str_key = ["longName", "sector", "industry", "longBusinessSummary", "quoteType"]

    df = pd.DataFrame(index=[ticker])
    df.index.name = "Ticker"
    for key in key_list:
        if key == "lastFiscalYearEnd":
            if key in info:
                df[key] = [pd.Timestamp(info[key], unit="s").date()]
            else:  # in case of missing lastFiscalYearEnd, set it to the last day of the previous year
                df[key] = [datetime.date(year=pd.Timestamp.now().year-1, month=12, day=31)]
        elif key == "longName":
            if key in info:
                df[key] = [info[key]]
            else:
                df[key] = [""]
        else:
            if key in info:
                if key in str_key:
                    df[key] = [str(info[key])]
                else:
                    df[key] = [pd.to_numeric(info[key], errors="coerce", downcast="float")]
    # lowerCamelCase -> Upper Camel Case
    df.columns = [_camel_to_title_case(col) for col in df.columns]
    return df


def _postprocess_fundamental(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    try:
        df = df.T
        df = df.sort_index(axis=1)
        df = df.reset_index(names="As Of Date")
        # append aditional data
        df["Ticker"] = ticker
        df["As Of Date"] = df["As Of Date"].dt.date  # remove time in "As Of Date"
        
        df = df.set_index(["Ticker", "As Of Date"])
    except Exception as e:
        import pickle
        with open(f"error_fundamental_{ticker}.pkl", "wb") as f:  # debug 용
            pickle.dump(df, f)
        raise e

    df = df.dropna(axis=1, thresh=len(df)//2)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _postprocess_estimates(df: pd.DataFrame, ticker: str, info: pd.DataFrame, value_type: str="EPS") -> pd.DataFrame:
    if info["Last Fiscal Year End"].item().month == 2 and info["Last Fiscal Year End"].item().day == 29:
        info["Last Fiscal Year End"] = info["Last Fiscal Year End"].item().replace(day=28)
    
    if info["Last Fiscal Year End"].item().month in [1, 2]:
        # 회계마감이 1월 또는 2월인 경우, 예측 연도를 +1 해주는게 맞음 (e.g. NVDA)
        this_year = pd.Timestamp.now().year + 1
    else:
        this_year = pd.Timestamp.now().year

    this_year = pd.Timestamp.now().year
    this_date = info.loc[:, "Last Fiscal Year End"].item().replace(year=this_year)
    next_date = info.loc[:, "Last Fiscal Year End"].item().replace(year=this_year+1)

    if df.empty:
        df = pd.DataFrame({
            "Ticker": ticker,
            "Fiscal Period": [this_date, next_date],
            f"{value_type} Estimate": [np.nan, np.nan],
            f"{value_type} YoY Growth": [np.nan, np.nan],
        })
        df = df.set_index(["Ticker", "Fiscal Period"])
    else:
        if not "avg" in df.columns:
            df["avg"] = np.nan
        if not "growth" in df.columns:
            df["growth"] = np.nan

        df = df.loc[["0y", "+1y"], ["avg", "growth"]]
        df = df.rename(
            index={"0y": this_date, "+1y": next_date},
            columns={"avg": f"{value_type} Estimate", "growth": f"{value_type} YoY Growth"},
        )
        df = df.reset_index(names="Fiscal Period")
        df["Ticker"] = ticker
        df = df.set_index(["Ticker", "Fiscal Period"])
    return df


def _postprocess_ohlcv(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    df = df.rename_axis("Date").reset_index()
    df["Ticker"] = ticker
    df = df.set_index(["Ticker", "Date"])
    df["Trading Value"] = df["Close"] * df["Volume"]
    return df[["Open", "High", "Low", "Close", "Volume", "Trading Value"]]


def _request_with_retry(yf_ticker: yf.Ticker, yfdata_type: YFDataType, max_retries: int, info: pd.DataFrame=None):
    ticker_name = yf_ticker.ticker

    time.sleep(1 + random.random())
    for retry in range(max_retries):
        try:
            if yfdata_type == "info":
                result = _postprocess_info(yf_ticker.info, ticker_name)
            elif yfdata_type == "income_statement":
                finance_df = yf_ticker.get_income_stmt(freq="yearly")
                result = _postprocess_fundamental(finance_df, ticker_name)
            elif yfdata_type == "income_statement_quarter":
                finance_df = yf_ticker.get_income_stmt(freq="quarterly")
                result = _postprocess_fundamental(finance_df, ticker_name)
            elif yfdata_type == "balance_sheet":
                finance_df = yf_ticker.get_balance_sheet(freq="yearly")
                result = _postprocess_fundamental(finance_df, ticker_name)
            elif yfdata_type == "balance_sheet_quarter":
                finance_df = yf_ticker.get_balance_sheet(freq="quarterly")
                result = _postprocess_fundamental(finance_df, ticker_name)
            elif yfdata_type == "cash_flow":
                finance_df = yf_ticker.get_cashflow(freq="yearly")
                result = _postprocess_fundamental(finance_df, ticker_name)
            elif yfdata_type == "cash_flow_quarter":
                finance_df = yf_ticker.get_cashflow(freq="quarterly")
                result = _postprocess_fundamental(finance_df, ticker_name)
            elif yfdata_type == "estimates":
                eps = _postprocess_estimates(yf_ticker.get_earnings_estimate(), ticker_name, info, "EPS")
                sales = _postprocess_estimates(yf_ticker.get_revenue_estimate(), ticker_name, info, "Sales")
                result = eps.join(sales, how="inner")
            elif yfdata_type == "ohlcv":
                result = _postprocess_ohlcv(yf_ticker.history(period="1y", raise_errors=True), ticker_name)
            return result

        except Exception as e:
            logging.warning(f"'{ticker_name}' 요청 실패 → 재시도: {e}")
            error_msg = traceback.format_exc()
            time.sleep((2 ** retry) + random.random())

    logging.warning(f"'{ticker_name}'의 '{yfdata_type}' 데이터 요청에 실패했습니다.")
    logging.warning(error_msg)
    return None


def _download_single_ticker(ticker: str, max_retries: int=10) -> tuple:
    ticker_data = {}
    yf_ticker = yf.Ticker(ticker)
    for yfdata_type in get_args(YFDataType):
        if yfdata_type == "info":
            info = _request_with_retry(yf_ticker, yfdata_type, max_retries)
            if info is None:
                ticker_data = None
                break
            else:
                ticker_data[yfdata_type] = info
        else:
            result = _request_with_retry(yf_ticker, yfdata_type, max_retries, info)
            if result is None:
                ticker_data = None
                break
            else:
                ticker_data[yfdata_type] = result
    return ticker_data


class YFDownloader:
    def __init__(self):
        self.data = {k: [] for k in get_args(YFDataType)}

    def download(self, tickers: str | list[str], max_workers: int = 8):
        if isinstance(tickers, str):
            tickers = [tickers]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for results_dict in tqdm(executor.map(_download_single_ticker, tickers), total=len(tickers)):
                if results_dict is not None:
                    for key, df in results_dict.items():
                        self.data[key].append(df)

    def save(self, save_dir: str):
        # Save data to parquet files
        os.makedirs(save_dir, exist_ok=True)
        for key, df_list in self.data.items():
            if df_list:
                combined_df = pd.concat(df_list, axis=0)
                save_path = os.path.join(save_dir, f"{key}.parquet")
                combined_df.to_parquet(save_path)
