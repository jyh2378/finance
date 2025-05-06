import traceback
import logging
import time
import random
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from fundamental import *
from utils import write_excel


def get_company_fundamental(ticker, random_request=True):
    """ticker 회사의 기본 fundamental 정보를 가져옵니다."""
    print(f"##### ticker:{ticker} start #####")
    try:
        extractor = YahooFinanceInfoExtractor()
        extractor.setup_ticker(ticker)
        name = extractor.get_company_name()
        data = {"ticker": ticker, "name": name}
        data.update({
            "섹터": extractor.get_sector(),
            "업종": extractor.get_industry(),
            "가격": extractor.get_price()
        })
        data.update({
            "작년 동분기 순수익": extractor.get_net_income(before=4),
            "직전분기 순수익": extractor.get_net_income(before=1),
            "현분기 순수익": extractor.get_net_income(before=0)
        })
        data.update({
            "작년 동분기 영업이익": extractor.get_operating_income(before=4),
            "직전분기 영업이익": extractor.get_operating_income(before=1),
            "현분기 영업이익": extractor.get_operating_income(before=0)
        })
        data.update({
            "작년 동분기 순매출": extractor.get_operating_revenue(before=4),
            "직전분기 순매출": extractor.get_operating_revenue(before=1),
            "현분기 순매출": extractor.get_operating_revenue(before=0)
        })
        data.update({
            "작년 동분기 영업활동현금흐름": extractor.get_operating_cashflow(before=4),
            "직전분기 영업활동현금흐름": extractor.get_operating_cashflow(before=1),
            "현분기 영업활동현금흐름": extractor.get_operating_cashflow(before=0)
        })
        data.update({
            "EPS": extractor.get_eps(),
            "BPS": extractor.get_bps(),
            "SPS": extractor.get_sps(),
            "ROE": extractor.get_roe(),
            "PEGR": extractor.get_pegr(),
            "PBR": extractor.get_pbr(),
            "PSR": extractor.get_psr(),
        })
    except:
        logging.error(f"~~~~~~~~~~ ticker:{ticker} error ~~~~~~~~~~")
        logging.error(traceback.format_exc())
        exit(1)
    
    if random_request:
        time.sleep(random.randrange(3, 5))

    return data


def make_base_sheet(excel_path):
    all_tickers = get_all_stocks_info()
    all_tickers_list = all_tickers["symbol"].to_list()
    df = []
    with ProcessPoolExecutor(max_workers=8) as executor:
        for data in executor.map(get_company_fundamental, all_tickers_list):
            df.append(data)
    write_excel(df, save_path=excel_path, sheet_name="Base", rewrite=True)


def make_growth_sheet(excel_path):
    base_df = pd.read_excel(excel_path, sheet_name="Base", index_col="ticker").dropna()
    compare_dict = {
        "작년 동분기 대비 순수익성장률": ("현분기 순수익", "작년 동분기 순수익"),
        "작년 동분기 영업이익성장률": ("현분기 영업이익", "작년 동분기 영업이익"),
        "작년 동분기 순매출성장률": ("현분기 순매출", "작년 동분기 순매출"),
        "작년 동분기 영업활동현금흐름성장률": ("현분기 영업활동현금흐름", "작년 동분기 영업활동현금흐름"),
        "직전분기 대비 순수익성장률": ("현분기 순수익", "직전분기 순수익"),
        "직전분기 영업이익성장률": ("현분기 영업이익", "직전분기 영업이익"),
        "직전분기 순매출성장률": ("현분기 순매출", "직전분기 순매출"),
        "직전분기 영업활동현금흐름성장률": ("현분기 영업활동현금흐름", "직전분기 영업활동현금흐름"),
        "ROE": "ROE",
        "PEGR(adjusted)": "PEGR",
        "PBR(adjusted)": "PBR",
        "PSR(adjusted)": "PSR",
    }
    growth_df = pd.DataFrame(index=base_df.index)
    for k, v in compare_dict.items():
        if isinstance(v, tuple):
            this, prev = v
            growth_df[k] = base_df.apply(lambda x: calc_growth_rate(x[this], x[prev]), axis=1)
        else:
            if k in ["PEGR(adjusted)", "PBR(adjusted)", "PSR(adjusted)"]:
                growth_df[k] = base_df[v].apply(lambda row: -1 * (row - base_df[v].max()) if row < 0 else row)
            else:
                growth_df[k] = base_df[v]
    growth_df = growth_df.round(4)
    write_excel(growth_df, save_path=excel_path, sheet_name="Growth")


def make_score_sheet(excel_path):
    growth_df = pd.read_excel(excel_path, sheet_name="Growth", index_col="ticker").dropna()
    rank_df = pd.DataFrame(columns=growth_df.columns, index=growth_df.index)
    for col in rank_df:
        if col in ["PEGR(adjusted)", "PBR(adjusted)", "PSR(adjusted)"]:
            rank_df[col] = growth_df[col].rank(method="min", ascending=True)
        else:
            rank_df[col] = growth_df[col].rank(method="min", ascending=False)
    write_excel(rank_df, save_path=excel_path, sheet_name="Rank")
    
    score_df = pd.DataFrame(columns=growth_df.columns, index=growth_df.index)
    for col in score_df:
        ranking_ratio = (len(rank_df.index) - rank_df[col] + 1) / len(rank_df.index)
        if col in ["ROE", "PEGR(adjusted)", "PBR(adjusted)", "PSR(adjusted)"]:
            score_df[col] = ranking_ratio * 15
        else:
            score_df[col] = ranking_ratio * 5
    score_df["총점"] = score_df.sum(axis=1)
    score_df = score_df.round(2)
    write_excel(score_df, save_path=excel_path, sheet_name="Score")


if __name__ == "__main__":

    # 로그 설정
    logging.basicConfig(filename='log.txt', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


    # make fundamental info sheet
    excel_path = f"stock_{get_today(to_str=True)}.xlsx"
    make_base_sheet(excel_path)
    make_growth_sheet(excel_path)
    make_score_sheet(excel_path)
