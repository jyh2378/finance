import yfinance as yf


def net_profit_growth_from_last_quarter(quarterly_financials):  # 직전분기 대비 순이익성장률
    financials = quarterly_financials
    if len(financials.columns) < 2 or "Net Income" not in financials.index:
        out = None
    else:
        this_quarter, prev_quarter = financials.columns[0], financials.columns[1]
        out = ((financials[this_quarter]["Net Income"] - financials[prev_quarter]["Net Income"]) / (financials[prev_quarter]["Net Income"] + 10e-10)) * 100
    return out

def operating_profit_growth_from_last_quarter(quarterly_financials):  # 직전분기 대비 영업이익성장률
    financials = quarterly_financials
    if len(financials.columns) < 2 or "Operating Income" not in financials.index:
        out = None
    else:
        this_quarter, prev_quarter = financials.columns[0], financials.columns[1]
        out = ((financials[this_quarter]["Operating Income"] - financials[prev_quarter]["Operating Income"]) / (financials[prev_quarter]["Operating Income"] + 10e-10)) * 100
    return out

def revenue_growth_from_last_quarter(quarterly_financials):    # 직전분기 대비 매출성장률
    financials = quarterly_financials
    if len(financials.columns) < 2 or "Total Revenue" not in financials.index:
        out = None
    else:
        this_quarter, prev_quarter = financials.columns[0], financials.columns[1]
        out = ((financials[this_quarter]["Total Revenue"] - financials[prev_quarter]["Total Revenue"]) / (financials[prev_quarter]["Total Revenue"] + 10e-10)) * 100
    return out

def operating_cash_flow_from_last_quarter(quarterly_cashflow):   # 직전분기 대비 영업활동현금흐름 증감량
    cashflow = quarterly_cashflow
    if len(cashflow) == 0 or len(cashflow.columns) < 2 or "Operating Cash Flow" not in cashflow.index:
        out = None
    else:
        this_quarter, prev_quarter = cashflow.columns[0], cashflow.columns[1]
        out = ((cashflow[this_quarter]["Operating Cash Flow"] - cashflow[prev_quarter]["Operating Cash Flow"]) / (cashflow[prev_quarter]["Operating Cash Flow"] + 10e-10)) * 100
    return out

def net_profit_growth_from_last_year(quarterly_financials):  # 작년 동분기 대비 순이익성장률
    financials = quarterly_financials
    if len(financials.columns) < 5 or "Net Income" not in financials.index:
        out = None
    else:
        this_quarter, prev_quarter = financials.columns[0], financials.columns[4]
        out = ((financials[this_quarter]["Net Income"] - financials[prev_quarter]["Net Income"]) / (financials[prev_quarter]["Net Income"] + 10e-10)) * 100
    return out

def operating_profit_growth_from_last_year(quarterly_financials):
    financials = quarterly_financials
    if len(financials.columns) < 5 or "Operating Income" not in financials.index:
        out = None
    else:
        this_quarter, prev_quarter = financials.columns[0], financials.columns[4]
        out = ((financials[this_quarter]["Operating Income"] - financials[prev_quarter]["Operating Income"]) / (financials[prev_quarter]["Operating Income"] + 10e-10)) * 100
    return out

def revenue_growth_from_last_year(quarterly_financials):
    financials = quarterly_financials
    if len(financials.columns) < 5 or "Total Revenue" not in financials.index:
        out = None
    else:
        this_quarter, prev_quarter = financials.columns[0], financials.columns[4]
        out = ((financials[this_quarter]["Total Revenue"] - financials[prev_quarter]["Total Revenue"]) / (financials[prev_quarter]["Total Revenue"] + 10e-10)) * 100
    return out

def operating_cash_flow_from_last_year(quarterly_cashflow):   # 영업활동현금흐름 증감량
    cashflow = quarterly_cashflow
    if len(cashflow) == 0 or len(cashflow.columns) < 5 or "Operating Cash Flow" not in cashflow.index:
        out = None
    else:
        this_quarter, prev_quarter = cashflow.columns[0], cashflow.columns[4]
        out = ((cashflow[this_quarter]["Operating Cash Flow"] - cashflow[prev_quarter]["Operating Cash Flow"]) / (cashflow[prev_quarter]["Operating Cash Flow"] + 10e-10)) * 100
    return out

def ROE(info):
    # 당기순이익(Net Income) / 자본(Equity)
    if "returnOnEquity" in info:
        return info["returnOnEquity"]
    else:
        return None

def PEGR(info):
    # PER / EPS 증가율
    if "pegRatio" in info:
        return info["pegRatio"]
    else:
        return None

def PBR(info):
    # = 주가 / [(총자산-총부채)/ 주식수]
    # = 시가총액(주가*주식 수) / 자본(총 자산-총 부채)
    # = 주가 / BPS(주당순자산)
    if "priceToBook" in info:
        return info["priceToBook"]
    else:
        return None

def PSR(info):
    # = 주가 / SPS[주당매출액]
    # = 주가 / (매출액 / 주식 수)
    # = 시가총액(주가*주식 수) / 매출액
    if "priceToSalesTrailing12Months" in info:
        return info["priceToSalesTrailing12Months"]
    else:
        return None
    
#####

def equity(quarterly_balance_sheet):
    balance = quarterly_balance_sheet
    if len(balance) == 0 or len(balance.columns) < 5 or "Stockholders Equity" not in balance.index:
        out = None
    else:
        this_quarter = balance.columns[0]
        out = balance[this_quarter]["Stockholders Equity"]
    return out

def EPS(ticker):
    company = yf.Ticker(ticker)
    # 1주당 순이익
    # = net income(earning, 당기 순이익) / 총 발행 주식수
    if "trailingEps" in company.info:
        out = company.info["trailingEps"]
    return out

def BPS():
    # (총자산-총부채) / 총 발행 주식수
    return

def SPS():
    # 총 매출액(total revenue) / 총 발행 주식수
    return

def PER():
    # market cap(시가총액) / net income(earning, 당기 순이익)
    # = 주가 / EPS
    return

def EPS_growth():
    # 증감율 = (시말 - 시초) / 시초
    return

