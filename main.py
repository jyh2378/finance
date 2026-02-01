from finance.datafetcher import USADataFetcher
from finance.utils import *


if __name__ == "__main__":
    set_logger("log.txt")

    all_tickers = get_all_usa_tickers()
    today = get_today(to_str=True, str_format="%y%m%d")

    data_fetcher = USADataFetcher()
    data_fetcher.fetch_base_data(all_tickers, save_dir=f"data/usa", max_workers=8)
    # data_fetcher.fetch_ohlcv(all_tickers, save_dir=f"data/usa")
    