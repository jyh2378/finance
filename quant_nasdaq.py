from concurrent.futures import ProcessPoolExecutor
import random
import time
import traceback

import pandas as pd
import yfinance as yf

from yf_func_legacy import *


financials_keys = {
    "net_profit_growth_from_last_quarter": "지난분기 대비 순수익성장률",
    "net_profit_growth_from_last_year": "작년 동분기 대비 순수익성장률",
    "operating_profit_growth_from_last_quarter": "지난분기 대비 영업이익성장률",
    "operating_profit_growth_from_last_year": "작년 동분기 대비 영업이익성장률",
    "revenue_growth_from_last_quarter": "지난분기 대비 매출성장률",
    "revenue_growth_from_last_year": "작년 동분기 대비 매출성장률",
}

cashflow_keys = {
    "operating_cash_flow_from_last_quarter": "지난분기 대비 영업활동현금흐름 변화",
    "operating_cash_flow_from_last_year": "작년 동분기 대비 영업활동현금흐름 변화",
}

info_keys = {
    "ROE": "ROE",
    "PEGR": "PEGR",
    "PBR": "PBR",
    "PSR": "PSR",
}

all_keys = {}
all_keys.update(financials_keys)
all_keys.update(cashflow_keys)
all_keys.update(info_keys)

def get_company_info(ticker, name):
    try:
        print(f"##### ticker:{ticker} start #####")

        financials = yf.Ticker(ticker).quarterly_financials
        cashflow = yf.Ticker(ticker).quarterly_cashflow
        info = yf.Ticker(ticker).info

        result_dict = {"ticker": ticker, "name": name}
        for k in financials_keys:
            result_dict[financials_keys[k]] = globals()[k](financials)
        for k in cashflow_keys:
            result_dict[cashflow_keys[k]] = globals()[k](cashflow)
        for k in info_keys:
            result_dict[info_keys[k]] = globals()[k](info)
    except:
        print(f"##### ticker:{ticker} error, replace to empty row #####")
        traceback.print_exc()
        result_dict = {"ticker": ticker}
        for k in financials_keys:
            result_dict[financials_keys[k]] = None
        for k in cashflow_keys:
            result_dict[cashflow_keys[k]] = None
        for k in info_keys:
            result_dict[info_keys[k]] = None

    print(f"##### ticker:{ticker} end #####")
    time.sleep(random.randrange(5, 10))

    return result_dict


def get_rank_and_score_df(df: pd.DataFrame):
    df = df.dropna()
    rank_df = pd.DataFrame(columns=df.columns, index=df.index)
    score_df = pd.DataFrame(columns=df.columns, index=df.index)

    def pegr_adjust(value):
        if value < 0:
            value += 10e10
        return value

    for col in rank_df.columns:
        if col in ["PEGR"]:  # PEGR은 마이너스 값이 나올 수 있어서 보정해야 함
            min_value = df[col].min()
            if min_value < 0:
                df[col+"_adjusted"] = df[col].apply(pegr_adjust)
                rank_df[col] = df[col+"_adjusted"].rank(method="min", ascending=True)
        elif col in ["PBR", "PSR"]:   
            rank_df[col] = df[col].rank(method="min", ascending=True)
        else:
            rank_df[col] = df[col].rank(method="min", ascending=False)

    for col in score_df.columns:
        ranking_ratio = (len(df.index) - rank_df[col] + 1) / len(df.index)
        if col in list(financials_keys.values()) + list(cashflow_keys.values()):
            score_df[col] = ranking_ratio * 5
        elif col in list(info_keys.values()):
            score_df[col] = ranking_ratio * 15
        else:
            continue

    score_df["총점"] = score_df.sum(axis=1)

    return rank_df, score_df


if __name__ == "__main__":
    from_local_file = False

    # nasdaq
    ticker_list = pd.read_csv("nasdaq_screener_1720875506190.csv")
    all_tickers = ticker_list["Symbol"]
    all_names = ticker_list["Name"]

    if from_local_file:
        df = pd.read_excel("basic.xlsx")
    else:
        max_workers = 8
        result_dict_list = []
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for result_dict in executor.map(get_company_info, all_tickers, all_names):
                result_dict_list.append(result_dict)
        df = pd.DataFrame.from_dict(result_dict_list)
        df.to_excel(f"basic.xlsx", freeze_panes=(1, 0))

    if "ticker" in df.columns:
        df = df.set_index("ticker")
    rank_df, score_df = get_rank_and_score_df(df)
    with pd.ExcelWriter(f"analysis.xlsx", engine = 'xlsxwriter') as writer:
        df.to_excel(writer, sheet_name="basic", index=True, freeze_panes=(1, 0))
        rank_df.to_excel(writer, sheet_name="rank", index=True, freeze_panes=(1, 0))
        score_df.to_excel(writer, sheet_name="score", index=True, freeze_panes=(1, 0))
