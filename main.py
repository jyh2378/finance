import datetime
import requests

from core import YahooFinanceInfoDownloader
from utils import *


if __name__ == "__main__":
    set_logger()

    all_tickers = get_all_usa_tickers()
    today = get_today(to_str=True, str_format="%y%m%d")

    yf_downloader = YahooFinanceInfoDownloader()
    yf_downloader.download(all_tickers, max_workers=8)
    yf_downloader.save(f"DB/usa/{today}")
