import os
import traceback
import string
import math

import pandas as pd
from natsort import natsort_keygen

from utils.date import get_today


def exception_handler(func):
    def inner_Function(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            print(f"{func.__name__} wrong: {e}")
            return None
    return inner_Function


def is_korean(char):
    # Hangul syllables (U+AC00 to U+D7AF) or Hangul Jamo (U+1100 to U+11FF)
    return (ord(char) >= 0xAC00 and ord(char) <= 0xD7A3) or (ord(char) >= 0x1100 and ord(char) <= 0x11FF)


def is_alphabet(char):
    # Uppercase A-Z (U+0041 to U+005A) or lowercase a-z (U+0061 to U+007A)
    return (ord(char) >= 0x0041 and ord(char) <= 0x005A) or (ord(char) >= 0x0061 and ord(char) <= 0x007A)


def is_number(char):
    return char.isdigit()


def measure_excel_col_length(df: pd.DataFrame):
    max_value_lengths = df.map(lambda x: len(str(x))).median()
    columns = df.columns
    col_lengths = {}
    for idx, col in enumerate(columns):
        title_length = 0
        for char in col:
            title_length += 1.5 if is_alphabet(char) else 0
            title_length += 2 if is_korean(char) else 0
        value_length = max_value_lengths[col]

        if title_length > value_length:
            col_lengths[string.ascii_uppercase[idx + 1]] = round(title_length)  # idx + 1은 인덱스 컬럼 A를 제외하기 위함
        else:
            col_lengths[string.ascii_uppercase[idx + 1]] = round(value_length)
    return col_lengths


def write_excel(df: pd.DataFrame | list[dict], save_path=None, sheet_name=None, rewrite=False):
    if isinstance(df, list):
        df = pd.DataFrame.from_dict(df)

    if "Ticker" in df.columns:
        df = df.set_index("Ticker")

    df = df.sort_index(key=natsort_keygen())

    if save_path is None:
        save_path = f"stock_{get_today(to_str=True)}.xlsx"

    if not rewrite and os.path.exists(save_path):
        sheet_dict = pd.read_excel(save_path, sheet_name=None, index_col="Ticker")
        if sheet_name in sheet_dict:
            sheet_dict.pop(sheet_name)  # 중복되는 name의 sheet는 다시 써야 하므로 제거

    new_sheet_name = f"Sheet{len(sheet_dict)+1}" if sheet_name is None else sheet_name

    with pd.ExcelWriter(save_path, engine="xlsxwriter") as writer:
        if not rewrite:
            for sheet_name, exist_df in sheet_dict.items():
                exist_df.to_excel(writer, sheet_name=sheet_name, freeze_panes=(1, 1))
                excel_col_length = measure_excel_col_length(exist_df)
                for alphabet_col, length in excel_col_length.items():
                    writer.sheets[sheet_name].set_column(f"{alphabet_col}:{alphabet_col}", length)

        df.to_excel(writer, sheet_name=new_sheet_name, freeze_panes=(1, 1))
        excel_col_length = measure_excel_col_length(df)
        for alphabet_col, length in excel_col_length.items():
            writer.sheets[new_sheet_name].set_column(f"{alphabet_col}:{alphabet_col}", length)