import logging
import datetime
import os
import random
import time
from functools import wraps
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import yfinance as yf
from yfinance import shared
from yfinance.exceptions import YFException
from tqdm import tqdm

def retry(max_retries: int = 10, base: int = 2, on_fail=None):
    """
    max_retries: 실패 시 재시도 횟수
    base: backoff base (sleep = base**attempt + jitter)
    on_fail: 실패 시 반환할 값 (기본값: None)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # args[0] = self, args[1] = ticker 라는 가정
            ticker_name = getattr(args[1], "ticker", "UNKNOWN") if len(args) > 1 else "UNKNOWN"
            for attempt in range(max_retries + 1):
                time.sleep((base ** attempt) + random.random())
                try:
                    return func(*args, **kwargs)
                except YFException as e:
                    last_exception = e
                except Exception as e:
                    logging.error(f"Error on Ticker: {ticker_name}")
                    raise e
            logging.error(f"Failed to fetch data for {ticker_name} after {max_retries} attempts: {last_exception}")

            if callable(on_fail):
                return on_fail(ticker_name)
            return on_fail
        
        return wrapper
    return decorator


class USADataFetcher():
    def __init__(self):
        self.info_string_keys = ["longName", "sector", "industry", "longBusinessSummary", "quoteType"]
        self.info_numeric_keys = ["sharesOutstanding", "marketCap", "enterpriseValue", "trailingPE",
                                  "forwardPE", "trailingEps", "forwardEps", "beta"]

    @staticmethod
    def _devide_per_shares(value_dict, num_shares):
        for key in ["avg", "low", "high", "yearAgoRevenue"]:
            if key in value_dict:
                value_dict[key] /= num_shares
        return value_dict

    @staticmethod
    def _save_data(data: list[dict], save_path: str) -> None:
        info_df = pd.DataFrame(data).set_index("ticker")
        info_df.sort_index(inplace=True)

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        info_df.to_parquet(save_path)

    @retry(max_retries=10, on_fail=lambda ticker: {"ticker": ticker})
    def _request_info(self, ticker: yf.Ticker) -> dict:
        ticker_name = ticker.ticker
        info = ticker.info
        if not info:
            logging.error(f"Info is empty for {ticker_name}")
            return {}

        info_result = {"ticker": ticker_name}

        # Process lastFiscalYearEnd (special case: timestamp)
        if "lastFiscalYearEnd" in info:
            info_result["lastFiscalYearEnd"] = pd.Timestamp(info["lastFiscalYearEnd"], unit="s").date()
        else:
            # Default to last day of previous year
            info_result["lastFiscalYearEnd"] = datetime.date(year=pd.Timestamp.now().year - 1, month=12, day=31)

        for key in self.info_string_keys:
            info_result[key] = str(info.get(key, ""))

        for key in self.info_numeric_keys:
            if key in info:
                info_result[key] = pd.to_numeric(info[key], errors="coerce", downcast="float")
        return info_result

    @retry(
        max_retries=10,
        on_fail=lambda ticker: ({"ticker": ticker}, {"ticker": ticker})
    )
    def _request_eps_estimates(self, ticker: yf.Ticker, info: dict) -> pd.DataFrame:
        ticker_name = ticker.ticker
        eps_estimate = ticker.get_earnings_estimate()
        if eps_estimate.empty:
            logging.info(f"EPS estimates are empty for {ticker_name}")
            return {}, {}

        if info["lastFiscalYearEnd"].month in [1, 2]:  # 회계마감이 1, 2월이면, 이미 1년 연도가 미뤄짐 (e.g. NVDA)
            this_date = info["lastFiscalYearEnd"].year
        else:
            this_date = info["lastFiscalYearEnd"].year + 1

        eps_estimate_this = {"ticker": ticker_name, "targetType": "eps", "targetYear": this_date, }
        eps_estimate_next = {"ticker": ticker_name, "targetType": "eps", "targetYear": this_date + 1}

        eps_estimate_this.update(eps_estimate.loc["0q"].to_dict())
        eps_estimate_next.update(eps_estimate.loc["+1q"].to_dict())

        return eps_estimate_this, eps_estimate_next

    @retry(
        max_retries=10,
        on_fail=lambda ticker: ({"ticker": ticker}, {"ticker": ticker})
    )
    def _request_sps_estimates(self, ticker: yf.Ticker, info: dict) -> pd.DataFrame:
        ticker_name = ticker.ticker
        revenue_estimate = ticker.get_revenue_estimate()
        if revenue_estimate.empty:
            logging.info(f"SPS estimates are empty for {ticker_name}")
            return {}, {}

        if info["lastFiscalYearEnd"].month in [1, 2]:  # 회계마감이 1, 2월이면, 이미 1년 연도가 미뤄짐 (e.g. NVDA)
            this_date = info["lastFiscalYearEnd"].year
        else:
            this_date = info["lastFiscalYearEnd"].year + 1

        num_shares = info.get("sharesOutstanding", False)
        if not num_shares:
            return {"ticker": ticker}, {"ticker": ticker}

        sps_this_value = self._devide_per_shares(revenue_estimate.loc["0q"].to_dict(), num_shares)
        sps_next_value = self._devide_per_shares(revenue_estimate.loc["+1q"].to_dict(), num_shares)

        sps_estimate_this = {"ticker": ticker_name, "targetType": "sps", "targetYear": this_date, }
        sps_estimate_next = {"ticker": ticker_name, "targetType": "sps", "targetYear": this_date + 1}

        sps_estimate_this.update(sps_this_value)
        sps_estimate_next.update(sps_next_value)

        return sps_estimate_this, sps_estimate_next

    def _fetch_single_data(self, ticker: yf.Ticker) -> dict:
        info = self._request_info(ticker)
        if not info:
            return None

        eps_estimate_this, eps_estimate_next = self._request_eps_estimates(ticker, info)
        sps_estimate_this, sps_estimate_next = self._request_sps_estimates(ticker, info)
        estimates = [eps_estimate_this, eps_estimate_next, sps_estimate_this, sps_estimate_next]

        return info, estimates
    
    def fetch_base_data(self, tickers: list[str], save_dir: str | None = None, max_workers: int = 8) -> pd.DataFrame:
        yf_tickers = [yf.Ticker(ticker) for ticker in tickers]
        
        infos = []
        estimates = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for results in tqdm(
                executor.map(self._fetch_single_data, yf_tickers), total=len(yf_tickers), desc="Fetching Base Data"
            ):
                if results is not None:
                    info, estimates = results
                    infos.append(info)
                    estimates.extend(estimates)

        # save data
        today = datetime.date.today()
        year, week_number, _ = today.isocalendar()
        today_str = datetime.date.today().strftime("%y%m%d")
        self._save_data(infos, os.path.join(save_dir, "info.parquet"))
        self._save_data(
            estimates, os.path.join(save_dir, "estimates", f"{year}-w{week_number:02}-{today_str}.parquet")
        )
        return

    def fetch_ohlcv(self, tickers: list[str], save_dir: str, max_retries: int = 5) -> pd.DataFrame:
        ohlcv_df = yf.download(tickers, period="2y", interval="1d", group_by='ticker')
        ohlcv_df = ohlcv_df.T
        for retry in range(max_retries):
            if shared._ERRORS:
                time.sleep(5 ** retry)
                failed_tickers = list(shared._ERRORS.keys())
                ohlcv_df = ohlcv_df.drop(index=failed_tickers, errors='ignore')
                shared._ERRORS.clear()
                retry_ohlcv_df = yf.download(failed_tickers, period="2y", interval="1d", group_by='ticker')
                if not retry_ohlcv_df.empty:
                    ohlcv_df = pd.concat([ohlcv_df, retry_ohlcv_df.T])
        ohlcv_df.sort_index(level=0, sort_remaining=False, inplace=True)

        ohlcv_df.to_parquet(os.path.join(save_dir, "ohlcv.parquet"))
        return ohlcv_df
    
    






