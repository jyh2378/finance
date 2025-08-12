# finance

## Valuation 기법 정리
### ROIC
```
# ROIC
= NOPLAT / IC

# NOPAT(≒NOPLAT)
= EBIT - Tax = EBIT * (1 - Tax)

# IC(Invested Capital) → 계산 방식이 여러가지
= 순운전자본(Working Capital) + 유형자산(Property, Plant, & Equipment)  # ROIC 계산에 좀 더 용이하다고 함(From Gemini)
= 자기자본(Shareholders' Equity) + 이자부채(장기차입금 + 단기차입금)  # Yahoo Finance 계산방식
```

### FCF Yield
```
# TODO
```

### DCF
```
# Free Cash Flow(FCF)
= Operating Cash Flow - Maintenance CapEx
= Net Income(NOPAT) + Depreciation & Amortization - ΔNet Working Capital - Maintenance CapEx
= (Revenue * Operation margin) - Depreciation & Amortization - (ΔAccounts Receivable + ΔInventories - ΔAccounts Payable) - (CapEx - Growth CapEx)
```

### PEGR Valuation
```
price = PER * EPS = (PEGR * EPS growth) * EPS

fair PER = sector PEGR * EPS growth
fair price = (fair PER / forward PER) * price

# 영업이익 적자 회사들은 PER 대신 PSR 사용 가능
```

### 성장률 추적 아이디어
```
# Earning(or Revenue)의 YoY 성장률 값이 매주 어떻게 변동하는지를 이용
3 weeks Trend = (this YoY Growth estimate) / (3week prev YoY Growth estimate) - 1
```


## 계정 항목 정리
#### 손익계산서(Income Statement)
- 매출: Revenue
- 매출 이익: Gross Profit(=Revenue - Cost of Goods Sold)
- 영업 이익: Operating Profit(=Gross Profit - Operating Expenses)
#### 대차대조표(Balance Sheet)
- 자산총계: Total Assets
- 순운전자본: Net Working Capital(=Accounts Receivable + Inventories - Accounts Payable)
- 매출채권: Accounts Receivable
- 재고자산: Inventories
- 매입채무: Accounts Payable
- 단기차입금: Short-Term Debt
- 장기차입금: Long-Term Debt
- 자기자본(=자본총계): Shareholders' Equity
- 자본금: Common Stock + Preferred Stock
- 자본잉여금: Paid-in Capital
- 이익잉여금: Retained Earnings
- 총자본: Total Capitalization(=Shareholders' Equity + Long-Term Debt)
#### 현금흐름표(Cash Flow)
- 감가상각 & 무형자산상각: Depreciation & Amortization
- CapEx: Capital Expenditure(=Property, Plant, & Equipment + Intangibles)

### 기타
- 순자산가치(Net Asset Value): (Total Assets - Total Lialilities) / 발행 주식 수

## 재무제표 보는법
### 대차대조표
- 현금이 총부채보다 적다? → 유동성 위기
- 매출보다 빠르게 늘어나는 매출채권 → 외상으로 판 매출 이후 실제 돈이 안들어올 수도
- 이익보다 빠르게 늘어나는 재고 → 수요 감소 신호
- 단기부채가 보유현금보다 많다? → 채무 상환 능력 부족
- 무형자산이 자산이 절반을 넘어? → 자산가치 변동성 커짐, 주로 엔터회사
- 기타 장기자산이 총 자산의 절반 이상? → 불투명한 자산
- 우선주가 많다? → 기업이 일반주주보다 우선주 투자자에게 유리한 구조로 자금조달
- 자기자본이 음수? → 자본잠식 상태=상장폐지 위험
