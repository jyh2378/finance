import requests

import pandas as pd


def normalize_ticker(ticker):
    if ticker == "BRK/A":
       ticker = "BRK-A"
    elif ticker == "BRK/B":
        ticker = "BRK-B"
    else:
        pass
    return ticker


def get_all_stocks_info():
    def remove_duplicate_tickers(df):
        # Create a base ticker column by removing anything after the first special character (if it exists)
        df['base_symbol'] = df['symbol'].str.split(r'[-/^]').str[0]
        # Create a mask that identifies rows with base tickers that should be kept
        base_ticker_mask = df.groupby('base_symbol')['symbol'].transform(lambda x: x.str.contains(r'[-/^]').any() and x.str.contains(r'[-/^]').sum() < len(x))
        # Keep only the base ticker rows or rows without any subseries
        filtered_df = df.loc[(df['symbol'] == df['base_symbol']) | ~base_ticker_mask].drop(columns='base_symbol')
        return filtered_df

    headers = {
        'authority': 'api.nasdaq.com',
        'accept': 'application/json, text/plain, */*',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
        'origin': 'https://www.nasdaq.com',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.nasdaq.com/',
        'accept-language': 'en-US,en;q=0.9',
    }

    params = (
        ('tableonly', 'true'),
        ('limit', '25'),
        ('offset', '0'),
        ('download', 'true'),
    )

    r = requests.get('https://api.nasdaq.com/api/screener/stocks', headers=headers, params=params)
    data = r.json()['data']
    df = pd.DataFrame(data['rows'], columns=data['headers'])
    df['volume'] = df['volume'].astype("Int64")
    df["marketCap"] = pd.to_numeric(df["marketCap"], errors="coerce")

    filtered_df = remove_duplicate_tickers(df)
    filtered_df = filtered_df.dropna(subset=["marketCap"])
    filtered_df = filtered_df[filtered_df["volume"] > 100]
    filtered_df = filtered_df.sort_values(["volume"], ascending=True)
    filtered_df = filtered_df.drop_duplicates(subset=["name"], keep="last")
    filtered_df = filtered_df.sort_values(["symbol"], ascending=True)
    filtered_df["symbol"] = filtered_df["symbol"].apply(normalize_ticker)
    return filtered_df