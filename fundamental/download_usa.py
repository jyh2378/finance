import logging
import os
import time
import random
import trace
import pickle
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
from tqdm import tqdm
import yfinance as yf

from utils import *


def _postprocess_info(info: dict, ticker: str):

    def camel_to_title_case(text):
        acronyms = {"PE": "Pe"}
        for word, upper_word in acronyms.items():
            text = text.replace(word, upper_word)
        spaced = re.sub(r'([A-Z])', r' \1', text)
        title_case = spaced.strip().title()
        return title_case

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
    df.columns = [camel_to_title_case(col) for col in df.columns]
    return df


def _postprocess_fundamental(df: pd.DataFrame, ticker: str):
    df = df.T
    df = df.sort_index(axis=1)
    df = df.reset_index(names="As Of Date")
    # append aditional data
    df["Ticker"] = ticker
    df["As Of Date"] = df["As Of Date"].dt.date  # remove time in "As Of Date"
    
    df = df.set_index(["Ticker", "As Of Date"])

    df = df.dropna(axis=1, thresh=len(df)//2)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _postprocess_ohlcv(df: pd.DataFrame, ticker: str):
    df = df[["Open", "High", "Low", "Close", "Volume"]].T
    df.index.name = "Type"
    df["Ticker"] = ticker
    df = df.reset_index().set_index(["Ticker", "Type"])
    df.columns = [str(col.date()) for col in df.columns]
    return df


def _postprocess_estimates(
        df: pd.DataFrame, ticker: str, info: pd.DataFrame, value_type: str="EPS"
    ):
    if info["Last Fiscal Year End"].item().month == 2 and info["Last Fiscal Year End"].item().day == 29:
        info["Last Fiscal Year End"] = info["Last Fiscal Year End"].item().replace(day=28)

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


def _request_with_retry(yf_ticker: yf.Ticker, download_type: str, max_retries: int, info: pd.DataFrame=None):
    ticker_name = yf_ticker.ticker
    time.sleep(1 + random.random())
    for retry in range(max_retries):
        try:
            if download_type == "info":
                result = _postprocess_info(yf_ticker.info, ticker_name)
                if result["Quote Type"].item() is None:
                    logging.warning(f"'{ticker_name}'는 Quote Type이 None입니다.")
                    return None
                elif result["Quote Type"].item().upper() == "ETF":
                    return None
                elif "ETF" in result["Long Name"].item().upper():
                    return None
                elif result["Quote Type"].item().upper() != "EQUITY":
                    logging.warning(f"'{ticker_name}'는 Quote Type {result['Quote Type'].item()}입니다.")
                    return None
                elif "Sector" not in result or pd.isna(result["Sector"].item()) or result["Sector"].item() == "":
                    return None
                elif result.isna().sum(axis=1).item() >= len(result.columns) - 2:
                    logging.warning(f"'{ticker_name}'는 info 데이터가 없습니다.")
                    return None

            elif download_type == "ohlcv":
                result = _postprocess_ohlcv(yf_ticker.history(period="5d", raise_errors=True), ticker_name)
            elif download_type == "income_statement":
                result = _postprocess_fundamental(yf_ticker.income_stmt, ticker_name)
            elif download_type == "income_statement_quarter":
                result = _postprocess_fundamental(yf_ticker.quarterly_income_stmt, ticker_name)
            elif download_type == "balance_sheet":
                result = _postprocess_fundamental(yf_ticker.balance_sheet, ticker_name)
            elif download_type == "balance_sheet_quarter":
                result = _postprocess_fundamental(yf_ticker.quarterly_balance_sheet, ticker_name)
            elif download_type == "cash_flow":
                result = _postprocess_fundamental(yf_ticker.cash_flow, ticker_name)
            elif download_type == "cash_flow_quarter":
                result = _postprocess_fundamental(yf_ticker.quarterly_cashflow, ticker_name)
            elif download_type == "estimates":
                eps = _postprocess_estimates(yf_ticker.get_earnings_estimate(), ticker_name, info, "EPS")
                sales = _postprocess_estimates(yf_ticker.get_revenue_estimate(), ticker_name, info, "Sales")
                result = eps.join(sales, how="inner")
            return result
        
        except yf.exceptions.YFPricesMissingError as e:
            logging.warning(f"'{ticker_name}'는 No data found 오류가 발생했습니다.")
            return None
        except AttributeError as e:
            logging.warning(f"'{ticker_name}'는 AttributeError가 발생했습니다.")
            return None
        except TypeError as e:
            logging.warning(f"'{ticker_name}'는 TypeError가 발생했습니다.")
            return None
        except IndexError as e:
            logging.warning(f"'{ticker_name}'는 IndexError가 발생했습니다.")
            return None
        except Exception as e:
            logging.warning(f"'{ticker_name}' 요청 실패 → 재시도: {e}")
            logging.warning(traceback.format_exc())
            time.sleep((2 ** retry) + random.random())

    logging.warning(f"'{ticker_name}'의 '{download_type}' 데이터 요청에 실패했습니다.")
    return None


def _download(ticker: str, max_retries: int=10):
    company = yf.Ticker(ticker)

    # download info
    info = _request_with_retry(company, "info", max_retries)
    if info is None:
        return None, None, None, None, None, None, None, None, None

    # download stock info
    price = _request_with_retry(company, "ohlcv", max_retries)
    if price is None:   
        return None, None, None, None, None, None, None, None, None
    
    # download income statement
    is_y = _request_with_retry(company, "income_statement", max_retries)
    is_q = _request_with_retry(company, "income_statement_quarter", max_retries)

    # download balance sheet
    bs_y = _request_with_retry(company, "balance_sheet", max_retries)
    bs_q = _request_with_retry(company, "balance_sheet_quarter", max_retries)

    # download cash flow
    cf_y = _request_with_retry(company, "cash_flow", max_retries)
    cf_q = _request_with_retry(company, "cash_flow_quarter", max_retries)

    # download estimates
    estimates = _request_with_retry(company, "estimates", max_retries, info)
                
    return info, price, is_y, is_q, bs_y, bs_q, cf_y, cf_q, estimates


class YahooFinanceInfoDownloader:
    def __init__(self):
        self.data = {
            "info": None,
            "ohlcv": None,
            "income_statement": None,
            "income_statement_quarter": None,
            "balance_sheet": None,
            "balance_sheet_quarter": None,
            "cash_flow": None,
            "cash_flow_quarter": None,
            "estimates": None,
        }

    def download(self, tickers: str | list[str], max_workers: int = 8):
        if isinstance(tickers, str):
            tickers = [tickers]

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for result in tqdm(executor.map(_download, tickers), total=len(tickers)):
                if result[0] is None or result[1] is None:
                    continue
                results.append(result)

        for key in self.data.keys():
            index = list(self.data.keys()).index(key)
            self.data[key] = pd.concat([res[index] for res in results], axis=0)

    def save(self, save_dir: str):
        # Save data to parquet files
        os.makedirs(save_dir, exist_ok=True)
        for key, df in self.data.items():
            if df is not None:
                save_path = os.path.join(save_dir, f"{key}.parquet")
                df.to_parquet(save_path)
