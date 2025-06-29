import os
import sys
import time
import random
from glob import glob
from concurrent.futures import ThreadPoolExecutor

PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_HOME)

import pandas as pd
from tqdm import tqdm
import yfinance as yf


def download_ohlcv(ticker: str, max_retries: int=10):
    company = yf.Ticker(ticker)
    time.sleep(1 + random.random())
    for retry in range(max_retries):
        try:
            ohlcv = company.history(period="1y", raise_errors=True)
        except Exception as e:
            time.sleep((2 ** retry) + random.random())
    ohlcv["Ticker"] = ticker
    ohlcv = ohlcv.reset_index().set_index(["Ticker", "Date"])
    return ohlcv


if __name__ == "__main__":
    parquet_paths = glob("DB/**/price.parquet")
    df = pd.read_parquet(parquet_paths[-1])
    tickers = df.index.to_list()

    df_list = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for result_df in tqdm(executor.map(download_ohlcv, tickers), total=len(tickers)):
            df_list.append(result_df)
    ohlcv_df = pd.concat(df_list, axis=0)
    ohlcv_df.to_parquet("ASSETS/ohlcv.parquet")
