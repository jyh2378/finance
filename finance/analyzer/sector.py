"""
Sector-based stock price analysis and visualization module.
"""

from pathlib import Path
from typing import Literal

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


class SectorAnalyzer:
    """
    Sector별 주가 분석 및 차트 시각화 클래스.
    
    usa_info.parquet와 usa_ohlcv.parquet 파일을 로드하여
    sector별 주가 차트를 생성합니다.
    """
    
    def __init__(self, data_dir: str | Path = "data"):
        """
        Args:
            data_dir: parquet 파일이 위치한 디렉토리 경로
        """
        self.data_dir = Path(data_dir)
        self._info_df: pd.DataFrame | None = None
        self._ohlcv_df: pd.DataFrame | None = None
        self._close_df: pd.DataFrame | None = None
    
    @property
    def info_df(self) -> pd.DataFrame:
        """종목 정보 DataFrame (lazy loading)"""
        if self._info_df is None:
            self._info_df = pd.read_parquet(self.data_dir / "usa_info.parquet")
        return self._info_df
    
    @property
    def ohlcv_df(self) -> pd.DataFrame:
        """OHLCV DataFrame (lazy loading)"""
        if self._ohlcv_df is None:
            self._ohlcv_df = pd.read_parquet(self.data_dir / "usa_ohlcv.parquet")
        return self._ohlcv_df
    
    @property
    def close_df(self) -> pd.DataFrame:
        """종가(Close) 데이터만 추출한 DataFrame"""
        if self._close_df is None:
            # MultiIndex에서 Close 가격만 추출
            close_data = self.ohlcv_df.xs("Close", level="Price")
            self._close_df = close_data.T  # 날짜를 index로 변환
            self._close_df.index = pd.to_datetime(self._close_df.index)
        return self._close_df
    
    @property
    def sectors(self) -> list[str]:
        """사용 가능한 sector 목록"""
        return self.info_df["sector"].replace("", pd.NA).dropna().unique().tolist()
    
    def get_tickers_by_sector(self, sector: str) -> list[str]:
        """
        특정 sector에 속한 ticker 목록 반환.
        
        Args:
            sector: sector 이름 (예: 'Technology', 'Healthcare')
            
        Returns:
            해당 sector의 ticker 리스트
        """
        mask = self.info_df["sector"] == sector
        return self.info_df[mask].index.tolist()
    
    def get_sector_prices(
        self, 
        sector: str, 
        normalize: bool = True,
        start_date: str | None = None,
        end_date: str | None = None
    ) -> pd.DataFrame:
        """
        특정 sector의 주가 데이터 반환.
        
        Args:
            sector: sector 이름
            normalize: True면 시작일 기준 100으로 정규화
            start_date: 시작 날짜 (YYYY-MM-DD)
            end_date: 종료 날짜 (YYYY-MM-DD)
            
        Returns:
            sector 내 종목들의 주가 DataFrame
        """
        tickers = self.get_tickers_by_sector(sector)
        
        # ohlcv_df에 존재하는 ticker만 필터링
        available_tickers = [t for t in tickers if t in self.close_df.columns]
        
        if not available_tickers:
            raise ValueError(f"No price data available for sector: {sector}")
        
        prices = self.close_df[available_tickers].copy()
        
        # 날짜 필터링
        if start_date:
            prices = prices[prices.index >= pd.to_datetime(start_date)]
        if end_date:
            prices = prices[prices.index <= pd.to_datetime(end_date)]
        
        # 정규화 (시작일 = 100)
        if normalize:
            first_valid = prices.apply(lambda x: x.dropna().iloc[0] if x.dropna().any() else pd.NA)
            prices = (prices / first_valid) * 100
        
        return prices
    
    def get_sector_index(
        self, 
        sector: str,
        method: Literal["equal", "market_cap"] = "equal",
        start_date: str | None = None,
        end_date: str | None = None
    ) -> pd.Series:
        """
        Sector 지수 계산 (동일가중 또는 시가총액 가중).
        
        Args:
            sector: sector 이름
            method: 'equal' (동일가중) 또는 'market_cap' (시가총액 가중)
            start_date: 시작 날짜
            end_date: 종료 날짜
            
        Returns:
            sector 지수 Series (시작일 = 100 기준)
        """
        prices = self.get_sector_prices(sector, normalize=True, start_date=start_date, end_date=end_date)
        
        if method == "equal":
            # 동일가중 평균
            sector_index = prices.mean(axis=1)
        elif method == "market_cap":
            # 시가총액 가중 평균
            tickers = [t for t in prices.columns if t in self.info_df.index]
            market_caps = self.info_df.loc[tickers, "marketCap"].fillna(0)
            weights = market_caps / market_caps.sum()
            sector_index = (prices[tickers] * weights).sum(axis=1)
        else:
            raise ValueError(f"Unknown method: {method}")
        
        sector_index.name = sector
        return sector_index
    
    def plot_sector_chart(
        self,
        sector: str,
        top_n: int | None = 10,
        normalize: bool = True,
        start_date: str | None = None,
        end_date: str | None = None,
        figsize: tuple[int, int] = (14, 8),
        show_index: bool = True,
        save_path: str | Path | None = None
    ) -> plt.Figure:
        """
        특정 sector의 주가 차트 생성.
        
        Args:
            sector: sector 이름
            top_n: 시가총액 상위 N개 종목만 표시 (None이면 전체)
            normalize: 정규화 여부
            start_date: 시작 날짜
            end_date: 종료 날짜
            figsize: 그래프 크기
            show_index: sector 지수 표시 여부
            save_path: 저장 경로 (None이면 저장 안함)
            
        Returns:
            matplotlib Figure 객체
        """
        # sector 내 ticker 가져오기
        tickers = self.get_tickers_by_sector(sector)
        available_tickers = [t for t in tickers if t in self.close_df.columns]
        
        # 시가총액 기준 정렬
        ticker_info = self.info_df.loc[
            self.info_df.index.isin(available_tickers), 
            ["longName", "marketCap"]
        ].sort_values("marketCap", ascending=False)
        
        if top_n:
            ticker_info = ticker_info.head(top_n)
        
        selected_tickers = ticker_info.index.tolist()
        
        # 주가 데이터 가져오기
        prices = self.get_sector_prices(
            sector, normalize=normalize, start_date=start_date, end_date=end_date
        )[selected_tickers]
        
        # 차트 생성
        fig, ax = plt.subplots(figsize=figsize)
        
        # 개별 종목 차트
        for ticker in selected_tickers:
            if ticker in prices.columns:
                name = ticker_info.loc[ticker, "longName"]
                label = f"{ticker} ({name[:20]}...)" if len(str(name)) > 20 else f"{ticker} ({name})"
                ax.plot(prices.index, prices[ticker], label=label, alpha=0.7, linewidth=1)
        
        # Sector 지수 추가
        if show_index:
            sector_index = self.get_sector_index(sector, method="equal", start_date=start_date, end_date=end_date)
            ax.plot(
                sector_index.index, sector_index.values, 
                label=f"{sector} Index (Equal Weight)", 
                color="black", linewidth=2.5, linestyle="--"
            )
        
        # 차트 꾸미기
        ax.set_title(f"{sector} Sector Stock Prices", fontsize=14, fontweight="bold")
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Normalized Price (Start = 100)" if normalize else "Price ($)", fontsize=12)
        ax.legend(loc="upper left", fontsize=8, ncol=2)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # 저장
        if save_path:
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
            print(f"Chart saved to: {save_path}")
        
        return fig
    
    def plot_all_sectors_comparison(
        self,
        method: Literal["equal", "market_cap"] = "equal",
        start_date: str | None = None,
        end_date: str | None = None,
        figsize: tuple[int, int] = (14, 8),
        save_path: str | Path | None = None
    ) -> plt.Figure:
        """
        모든 sector 지수 비교 차트 생성.
        
        Args:
            method: 지수 계산 방식
            start_date: 시작 날짜
            end_date: 종료 날짜
            figsize: 그래프 크기
            save_path: 저장 경로
            
        Returns:
            matplotlib Figure 객체
        """
        fig, ax = plt.subplots(figsize=figsize)
        
        for sector in self.sectors:
            try:
                sector_index = self.get_sector_index(
                    sector, method=method, start_date=start_date, end_date=end_date
                )
                ax.plot(sector_index.index, sector_index.values, label=sector, linewidth=1.5)
            except ValueError as e:
                print(f"Skipping {sector}: {e}")
                continue
        
        # 차트 꾸미기
        weight_label = "Equal Weight" if method == "equal" else "Market Cap Weight"
        ax.set_title(f"All Sectors Comparison ({weight_label})", fontsize=14, fontweight="bold")
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Index (Start = 100)", fontsize=12)
        ax.legend(loc="upper left", fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # 저장
        if save_path:
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
            print(f"Chart saved to: {save_path}")
        
        return fig


# 편의 함수
def plot_sector(
    sector: str,
    data_dir: str = "data",
    top_n: int = 10,
    save_path: str | None = None
) -> plt.Figure:
    """
    간편하게 sector 차트를 그리는 함수.
    
    Example:
        >>> plot_sector("Technology", top_n=5)
        >>> plt.show()
    """
    analyzer = SectorAnalyzer(data_dir)
    return analyzer.plot_sector_chart(sector, top_n=top_n, save_path=save_path)


def plot_all_sectors(
    data_dir: str = "data",
    method: Literal["equal", "market_cap"] = "equal",
    save_path: str | None = None
) -> plt.Figure:
    """
    모든 sector 비교 차트를 그리는 함수.
    
    Example:
        >>> plot_all_sectors()
        >>> plt.show()
    """
    analyzer = SectorAnalyzer(data_dir)
    return analyzer.plot_all_sectors_comparison(method=method, save_path=save_path)
