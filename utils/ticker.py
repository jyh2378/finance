import re
import requests

import pandas as pd


def to_raw_char(char):
    if char in ['+', '-', '*', '/']:
        return rf"\{char}"
    return char


def remove_parentheses(text):
    # 소괄호로 둘러싼 내용을 제거
    text = re.sub(r'\(.*?\)', '', text)
    text = re.sub(r'\s+', ' ', text).strip()  # 중복된 공백 제거 및 양쪽 공백 제거
    return text


def get_start_words(text):
    if text.lower().startswith("bank of"):
        num_words = 3
    elif text.lower().startswith("abrdn"):
        num_words = 3
    else:
        num_words = 2
    word_list = text.split()
    words_text = " ".join(word_list[:num_words])
    return words_text


def _get_special_ticker(ticker_series):
    mask = ~ticker_series.str.contains(r"common|Common")
    mask = mask | ticker_series.str.contains(r"Class B|Class C|Series B|Series C")
    return ticker_series[mask].index.tolist()


def _get_duplicate_usa_tickers(usa_df):
    duplicate_tickers = set()
    criteria = usa_df["Security Name"].apply(remove_parentheses)
    for row in usa_df.itertuples():
        base = row[0]
        if base in duplicate_tickers:
            continue
        security_name = remove_parentheses(row[1])
        target_name = to_raw_char(get_start_words(security_name))
        target_criteria = criteria[criteria.index != base]  # 자기 자신 제외
        mask = target_criteria.str.startswith(target_name)
        if mask.any():
            duplicate_candidates = target_criteria.index[mask].tolist()
            for dup_cand in duplicate_candidates:
                if dup_cand.startswith(base):
                    duplicate_tickers.add(dup_cand)
            special_tickers = _get_special_ticker(usa_df.loc[[base] + duplicate_candidates, "Security Name"])
            duplicate_tickers.update(special_tickers)

    return list(duplicate_tickers)


def get_all_usa_tickers(do_filter=True, as_df=False):
    # 1) 원천 파일 URL
    url_nasdaq = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
    url_other  = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

    # 2) NASDAQ 전체 상장 (ETF/TestIssue 제외)
    nasdaq = pd.read_csv(url_nasdaq, sep="|", dtype=str)
    nasdaq = nasdaq[~nasdaq["Symbol"].str.startswith("File Creation Time", na=False)]
    nasdaq = nasdaq[(nasdaq["ETF"] == "N") & (nasdaq["Test Issue"] == "N")]
    nasdaq = nasdaq[~nasdaq['Symbol'].isna()]

    # 3) NYSE+AMEX 전체 상장 (ETF/TestIssue 제외)
    other = pd.read_csv(url_other, sep="|", dtype=str)
    other = other[~other.iloc[:,0].str.startswith("File Creation Time", na=False)]
    nyse_amex = other[other["Exchange"].isin(["N","A"])]                      # N=NYSE, A=NYSE American
    nyse_amex = nyse_amex[(nyse_amex["ETF"] == "N") & (nyse_amex["Test Issue"] == "N")]
    # 표준적으로 CQS Symbol을 표시용 티커로 많이 씁니다.
    nyse_amex = nyse_amex[~nyse_amex['CQS Symbol'].isna()]

    # 4) NASDAQ과 NYSE+AMEX 통합
    nasdaq_df = nasdaq.rename(columns={'Symbol': 'Ticker'})
    nyse_amex_df = nyse_amex.rename(columns={'CQS Symbol': 'Ticker'})
    usa_df = pd.concat([nasdaq_df, nyse_amex_df], ignore_index=True, sort=True)
    usa_df = usa_df.drop_duplicates(subset='Ticker', keep='first').reset_index(drop=True)
    usa_df = usa_df.set_index('Ticker').sort_index()
    usa_df = usa_df[["Security Name", "ETF", "Test Issue", "Round Lot Size"]]

    # 5) 중복 티커 (우선주, rights, Units, Warrants 등) 제거
    if do_filter:
        # 1. ABRpD 패턴 제거
        mask = usa_df.index.str.contains(rf'^[A-Z]+[p].+$')
        usa_df = usa_df[~mask]
        # 2. AACT.U, AACT.WS 패턴 제거
        mask = usa_df.index.str.contains(rf'^[A-Z]+[.][UW][S]?$')
        usa_df = usa_df[~mask]
        # 3. NE.WS.A 패턴 제거
        mask = usa_df.index.str.contains(rf'^[A-Z]+[.][A-Z]+[.][A-Z]+$')
        usa_df = usa_df[~mask]
        # 4. AKO.A, AKO.B가 존재할 경우, AKO.A만 남기고 그 외는 제거
        mask = usa_df.index.str.contains(rf'^[A-Z]+[.][^A]$')
        usa_df = usa_df[~mask]
        # 5. 보통주가 아닌 특수주식 티커 제거
        duplicate_tickers = _get_duplicate_usa_tickers(usa_df)
        usa_df = usa_df[~usa_df.index.isin(duplicate_tickers)]
        usa_df.index = usa_df.index.str.replace(".", "-")

    if as_df:
        return usa_df
    else:
        return usa_df.index.tolist()