from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List, Any
import pandas as pd
from dataclasses import dataclass
import logging

from .models import Bar, Order, Trade, Account, OrderSide, TargetPosition, Fundamental

class Strategy(ABC):
    # 更新基础财务、历史行情、资讯（若有）、除息除权等信息
    # 每天开盘前调用一次
    @abstractmethod
    def on_fundamentals(self, date: str, infos: Dict[str, Fundamental]): # 获取基本面、资讯等信息
        pass

    @abstractmethod
    def on_universe(self, data: List[Bar]) -> List[TargetPosition]:
        pass

class BaseStrategy(Strategy):
    def __init__(self, account: Account):
        self._account = account
        self._date = None
        self._infos: Dict[str, Fundamental] = {}
        self._bars: Dict[str, List[Bar]] = {}
        self._klines: Dict[str, pd.DataFrame] = {}

    def _merge_bars(self, forward_adjusted_kline_data: pd.DataFrame, bars: List[Bar]) -> pd.DataFrame:
        bar_dict = {'symbol': [], 'date': [], 'open_price': [], 'high_price': [], 'low_price': [], 'close_price': [], 'volume': [], 'turnover': [], 'change_percent': []}
        for bar in bars:
            bar_dict['symbol'].append(bar.symbol)
            bar_dict['date'].append(bar.end_timestamp)
            bar_dict['open_price'].append(str(bar.open))
            bar_dict['high_price'].append(str(bar.high))
            bar_dict['low_price'].append(str(bar.low))
            bar_dict['close_price'].append(str(bar.close))
            bar_dict['volume'].append(str(bar.volume))
            bar_dict['turnover'].append('0')
            bar_dict['change_percent'].append('0')
        return pd.concat([forward_adjusted_kline_data, pd.DataFrame(bar_dict)], ignore_index=True).drop_duplicates(subset=['date'], keep='first')

    def on_fundamentals(self, date: str, infos: Dict[str, Fundamental]):
        for symbol, position in self._account.positions.items():
            if symbol not in infos and position.quantity > Decimal('0'):
                raise ValueError(f"Symbol {symbol} not found in infos")
        self._date = date
        self._infos = infos

    def on_universe(self, data: List[Bar]) -> List[TargetPosition]:
        if self._infos is None:
            raise ValueError("Fundamentals data not set. Call on_fundamentals() first.")
       
        for bar in data:
            if bar.symbol not in self._infos:
                logging.error(f'symbol: {bar.symbol} not in infos, skip')
                continue
            if bar.symbol not in self._bars:
                self._bars[bar.symbol] = []
            self._bars[bar.symbol].append(bar)
            
            # 合并历史k线与当前bar数据
            self._klines[bar.symbol] = self._merge_bars(
                self._infos[bar.symbol].forward_adjusted_kline_data,
                self._bars[bar.symbol]
            )

        return self.calculate_target_positions()

    @abstractmethod
    def calculate_target_positions(self) -> List[TargetPosition]:
        """计算目标持仓"""
        pass

class TestStrategy(BaseStrategy):
    def __init__(self, account):
        super().__init__(account)

    def calculate_target_positions(self) -> List[TargetPosition]:
        if self._date == '2015-08-13':
            # 模拟在2015-08-13时(交易在下一天开盘)，买入000001.SZ和000002.SZ各100股
            return [
                TargetPosition(symbol='000001.SZ', quantity=Decimal('100')),
                TargetPosition(symbol='000002.SZ', quantity=Decimal('100'))
            ]
        if self._date == '2016-08-10':
            # 模拟在2016-08-10时（交易在下一天开盘），卖出000001.SZ和000002.SZ
            return [
                TargetPosition(symbol='000001.SZ', quantity=Decimal('0')),
                TargetPosition(symbol='000002.SZ', quantity=Decimal('50'))
            ]
        return [] # 默认不操作任何股票