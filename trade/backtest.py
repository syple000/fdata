
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Iterator, Any
import os

from .models import *
from .trading_system import TradingSystem

class DataFeed:
    def __init__(self, archive_path: str):
        self._historical_data_daily_none: Dict[str, pd.DataFrame] = {}
        self._historical_data_min5_none: Dict[str, pd.DataFrame] = {}
        self._historical_data_min15_none: Dict[str, pd.DataFrame] = {}
        self._historical_data_min30_none: Dict[str, pd.DataFrame] = {}
        self._historical_data_min60_none: Dict[str, pd.DataFrame] = {}
        self._financial_data: Dict[str, pd.DataFrame] = {}
        self._dividend_info: Dict[str, pd.DataFrame] = {}

    def _load_data(self, archive_path: str) -> None:
        # 数据存储目录 {archive_path}/{symbol}/XXX.csv
        symbols = os.listdir(archive_path)
        for symbol in symbols:
            symbol_path = os.path.join(archive_path, symbol)
            if not os.path.isdir(symbol_path):
                continue
            
            # 读取日线数据
            daily_file = os.path.join(symbol_path, 'historical_data_DAILY_NONE.csv')
            if os.path.exists(daily_file):
                df = pd.read_csv(daily_file, dtype=str)
                self._historical_data_daily_none[symbol] = df

            # 读取分钟数据
            for period in ['MIN5', 'MIN15', 'MIN30', 'MIN60']:
                min_file = os.path.join(symbol_path, f'historical_data_{period}_NONE.csv')
                if os.path.exists(min_file):
                    df = pd.read_csv(min_file, dtype=str)
                    getattr(self, f'_historical_data_{period.lower()}_none')[symbol] = df

            # 读取财务数据
            financial_file = os.path.join(symbol_path, 'financial.csv')
            if os.path.exists(financial_file):
                df = pd.read_csv(financial_file, dtype=str)
                self._financial_data[symbol] = df

            # 读取分红信息
            dividend_file = os.path.join(symbol_path, 'dividend.csv')
            if os.path.exists(dividend_file):
                df = pd.read_csv(dividend_file, dtype=str)
                self._dividend_info[symbol] = df
