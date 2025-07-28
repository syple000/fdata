from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List, Any
import pandas as pd
from dataclasses import dataclass

from .models import Bar, Order, Trade, Account, OrderSide, TargetPosition

class Strategy(ABC):
    # 更新基础财务、历史行情、资讯（若有）、除息除权等信息
    # 每天开盘前调用一次
    @abstractmethod
    def on_fundamentals(self, infos: Dict[str, Any]): # 获取基本面、资讯等信息
        pass

    @abstractmethod
    def on_universe(self, data: List[Bar]) -> List[TargetPosition]:
        pass

class TestStrategy(Strategy):
    def __init__(self, account: Account):
        self._account = account
        self._infos = None

    def _get_symbol_status(self, symbol: str) -> Dict[str, Any]:
        info = self._infos.get(symbol, {})
        financial_data = info.get('financial_data', None)
        dividend_info = info.get('dividend_info', None)
        forward_adjusted_kline_data = info.get('forward_adjusted_kline_data', None)
        position = self._account.positions.get(symbol, None)
        bars = info.get('bars', [])

        # 将bars合并到forward_adjusted_kline_data中
        df = forward_adjusted_kline_data.copy() if forward_adjusted_kline_data is not None else pd.DataFrame(columns=['symbol', 'date', 'open_price', 'high_price','low_price', 'close_price','volume','turnover','change_percent'])
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
        df = pd.concat([df, pd.DataFrame(bar_dict)], ignore_index=True).drop_duplicates(subset=['date'], keep='first')

        return {
            'financial_data': financial_data,
            'dividend_info': dividend_info,
            'kline_data': df, # 前复权
            'position': position,
        }

    def on_fundamentals(self, infos: Dict[str, Any]):
        '''
        数据格式如下：
        {
            '000001.SZ': {
                'financial_data': pd.DataFrame(...),
                'dividend_info': pd.DataFrame(...),
                'kline_data': pd.DataFrame(...),
                'forward_adjusted_kline_data': pd.DataFrame(...),
                'bars': [],
            }
        }
        '''
        for symbol, position in self._account.positions.items():
            if symbol not in infos and position.quantity > Decimal('0'):
                raise ValueError(f"Symbol {symbol} not found in infos")
        self._infos = infos

    def on_universe(self, data: List[Bar]) -> List[TargetPosition]:
        if self._infos is None:
            raise ValueError("Fundamentals data not set. Call on_fundamentals() first.")
       
        symbol_status = {}
        for bar in data:
            if bar.symbol not in self._infos:
                self._infos[bar.symbol] = {}
            if 'bars' not in self._infos[bar.symbol]:
                self._infos[bar.symbol]['bars'] = []
            self._infos[bar.symbol]['bars'].append(bar)
            symbol_status[bar.symbol] = self._get_symbol_status(bar.symbol)

        # todo 分析收益/风险，产出目标持仓
        if len(data) > 0 and data[0].end_timestamp == '2025-07-16 10:00:00':
            # 模拟在2025-07-16 10:00:00时，买入000001.SZ和000002.SZ各100股
            return [
                TargetPosition(symbol='000001.SZ', quantity=Decimal('100')),
                TargetPosition(symbol='000002.SZ', quantity=Decimal('100'))
            ]
        if len(data) > 0 and data[0].end_timestamp == '2025-07-21 11:00:00':
            # 模拟在2025-07-16 11:00:00时，卖出000001.SZ和000002.SZ
            return [
                TargetPosition(symbol='000001.SZ', quantity=Decimal('0')),
                TargetPosition(symbol='000002.SZ', quantity=Decimal('50'))
            ]
        target_positions = []
        return target_positions

