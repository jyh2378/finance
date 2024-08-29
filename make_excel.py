import traceback
import logging

from natsort import natsorted
import numpy as np
import pandas as pd

from yf_func import *
from krx_func import *
from utils import *


logging.basicConfig(filename="log.txt")

def make_row(ticker, nation="us", random_request=True):
    if nation.lower() == "us":
        info_extractor = YahooFinanceInfoExtractor()
    elif nation.lower() == "ko":
        info_extractor = DartInfoExtractor()
    else:
        raise RuntimeError
    
    try:
        success_download = info_extractor.download_fs(ticker)
    except Exception as e:
        print(f"ERROR: {ticker}")
        print(traceback.format_exc())
        exit()
    if success_download is False:
        return
    else:
        name = info_extractor.get_company_name(ticker)
        data_row = {"ticker": ticker, "name": name}

    try:
        this_quarter, prev_quarter, last_year_quarter = info_extractor.get_net_income(ticker)
        data_row.update({
            "작년 동분기 순수익": last_year_quarter,
            "직전분기 순수익": prev_quarter,
            "현분기 순수익": this_quarter
        })

        this_quarter, prev_quarter, last_year_quarter = info_extractor.get_operating_income(ticker)
        data_row.update({
            "작년 동분기 영업이익": last_year_quarter,
            "직전분기 영업이익": prev_quarter,
            "현분기 영업이익": this_quarter
        })

        this_quarter, prev_quarter, last_year_quarter = info_extractor.get_total_revenue(ticker)
        data_row.update({
            "작년 동분기 순매출": last_year_quarter,
            "직전분기 순매출": prev_quarter,
            "현분기 순매출": this_quarter
        })

        this_quarter, prev_quarter, last_year_quarter = info_extractor.get_operating_cashflow(ticker)
        data_row.update({
            "작년 동분기 영업활동현금흐름": last_year_quarter,
            "직전분기 영업활동현금흐름": prev_quarter,
            "현분기 영업활동현금흐름": this_quarter
        })

        data_row.update({
            "ROE": info_extractor.ROE(ticker),
            "PEGR": info_extractor.PEGR(ticker),
            "PBR": info_extractor.PBR(ticker),
            "PSR": info_extractor.PSR(ticker),
        })
    except Exception as e:
        logging.info(f"Error on {ticker}")
        logging.info(f"{traceback.format_exc()}\n")
        return None

    logging.info(f"##### ticker:{ticker} end #####")
    if random_request:
        time.sleep(random.randrange(5, 10))

    return data_row


def make_base_excel(ticker_list_file_path=None, save_path=None):
    if ticker_list_file_path:
        ticker_list = pd.read_csv(ticker_list_file_path)
        all_tickers = normalize_ticker(ticker_list["Symbol"])

    nations = ["us" for _ in range(len(all_tickers))]
    random_request = [True for _ in range(len(all_tickers))]

    data_dict_list = []
    with ProcessPoolExecutor(max_workers=8) as executor:
        for result_dict in executor.map(make_row, all_tickers, nations, random_request):
            if result_dict is not None:
                data_dict_list.append(result_dict)
    df = pd.DataFrame.from_dict(data_dict_list)
    df = df.set_index("ticker")

    if save_path is None:
        save_path = f"stock_{get_today(to_str=True)}.xlsx"
    df.to_excel(save_path, sheet_name="base", freeze_panes=(1, 0))
    return save_path


def analysis(excel_path):
    base_df = pd.read_excel(excel_path, index_col="ticker")
    dropped_df = base_df.dropna()
    compare_dict = {
        "작년 동분기 대비 순수익성장률": ("현분기 순수익", "작년 동분기 순수익"),
        "직전분기 대비 순수익성장률": ("현분기 순수익", "직전분기 순수익"),
        "작년 동분기 영업이익성장률": ("현분기 영업이익", "작년 동분기 영업이익"),
        "직전분기 영업이익성장률": ("현분기 영업이익", "직전분기 영업이익"),
        "작년 동분기 순매출성장률": ("현분기 순매출", "작년 동분기 순매출"),
        "직전분기 순매출성장률": ("현분기 순매출", "직전분기 순매출"),
        "작년 동분기 영업활동현금흐름성장률": ("현분기 영업활동현금흐름", "작년 동분기 영업활동현금흐름"),
        "직전분기 영업활동현금흐름성장률": ("현분기 영업활동현금흐름", "직전분기 영업활동현금흐름"),
        "ROE": "ROE",
        "PEGR": "PEGR",
        "PBR": "PBR",
        "PSR": "PSR",
    }
    info_df = pd.DataFrame(index=dropped_df.index)
    info_df["name"] = dropped_df["name"]
    for k, v in compare_dict.items():
        if isinstance(v, tuple):
            this, prev = v
            info_df[k] = dropped_df.apply(lambda x: calc_growth_rate(x[this], x[prev]), axis=1)
        else:
            info_df[k] = dropped_df[v]
            if k == "PEGR":
                info_df[k+"_adjusted"] = info_df[k].apply(lambda row: -1000 * row if row < 0 else row)

    rank_df = pd.DataFrame(columns=compare_dict.keys(), index=info_df.index)
    for col in compare_dict.keys():
        if col in ["PEGR"]:  # PEGR은 마이너스 값이 나올 수 있어서 보정해야 함
            rank_df[col] = info_df[col+"_adjusted"].rank(method="min", ascending=True)
        elif col in ["PBR", "PSR"]:   
            rank_df[col] = info_df[col].rank(method="min", ascending=True)
        else:
            rank_df[col] = info_df[col].rank(method="min", ascending=False)

    score_df = pd.DataFrame(columns=compare_dict.keys(), index=info_df.index)
    for col in compare_dict.keys():
        ranking_ratio = (len(info_df.index) - rank_df[col] + 1) / len(info_df.index)
        if col in ["ROE", "PEGR", "PBR", "PSR"]:
            score_df[col] = ranking_ratio * 15
        else:
            score_df[col] = ranking_ratio * 5
    score_df["총점"] = score_df.loc[:, score_df.columns != "name"].sum(axis=1)
    
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        base_df.to_excel(writer, sheet_name="base", freeze_panes=(1, 1))    # 다시 안쓰면 날아감
        info_df.to_excel(writer, sheet_name="info", freeze_panes=(1, 1))
        rank_df.to_excel(writer, sheet_name="rank", freeze_panes=(1, 1))
        score_df.to_excel(writer, sheet_name="score", freeze_panes=(1, 1))

        writer.sheets["base"].set_column("C:N", 18)
        writer.sheets["info"].set_column("C:J", 18)
        writer.sheets["rank"].set_column("C:J", 18)
        writer.sheets["score"].set_column("C:J", 18)


if __name__ == "__main__":
    file_path = make_base_excel("assets/nasdaq_screener_1723278506670.csv", save_path=f"{get_today().strftime('%y%m%d')}_stock.xlsx")
    analysis(file_path)
