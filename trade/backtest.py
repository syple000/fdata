
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Iterator, Any

from .models import *
from .trading_system import TradingSystem

# 股票回测系统模块：包含数据读取、事件驱动、策略接口、回测引擎和历史收益曲线统计

class Bar:
    """一分钟/日线/K线 等通用 Bar 对象"""
    def __init__(
        self,
        symbol: str,
        datetime: datetime,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: int
    ):
        self.symbol = symbol
        self.datetime = datetime
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

class DataFeed:
    """简单的 CSV 行情读取器，支持日线、分钟线、tick"""
    def __init__(self):
        self._data: Dict[str, pd.DataFrame] = {}

    def load_csv(self, symbol: str, path: str, datetime_col: str = "datetime") -> None:
        df = pd.read_csv(path, parse_dates=[datetime_col])
        df.sort_values(datetime_col, inplace=True)
        self._data[symbol] = df

    def bars(self, symbol: str) -> Iterator[Bar]:
        """逐条返回 Bar"""
        df = self._data.get(symbol)
        if df is None:
            return
        for row in df.itertuples():
            yield Bar(
                symbol=symbol,
                datetime=row.datetime,
                open=Decimal(str(row.open)),
                high=Decimal(str(row.high)),
                low=Decimal(str(row.low)),
                close=Decimal(str(row.close)),
                volume=int(row.volume)
            )

class Strategy:
    """策略基类：用户继承并实现 on_bar 或 on_tick"""
    def on_bar(self, bar: Bar, ts: "TradingSystem") -> None:
        """Bar 事件触发"""
        pass

    def on_tick(self, tick: Any, ts: "TradingSystem") -> None:
        """Tick 事件触发"""
        pass

class BacktestEngine:
    """回测引擎：驱动 DataFeed，触发策略并模拟撮合成交"""
    def __init__(
        self,
        data_feed: DataFeed,
        strategy: Strategy,
        trading_system: "TradingSystem"
    ):
        self.data_feed = data_feed
        self.strategy = strategy
        self.ts = trading_system
        self.equity_curve: List[Decimal] = []

    def run(self, symbol: str) -> None:
        """对指定标的逐 Bar 回测"""
        for bar in self.data_feed.bars(symbol):
            # 策略决策
            self.strategy.on_bar(bar, self.ts)

            # 模拟成交：对所有 SUBMITTED 状态订单以当前收盘价全部成交
            for order in list(self.ts.orders.values()):
                if order.status == OrderStatus.SUBMITTED:
                    self.ts.execute_trade(
                        order_id=order.order_id,
                        quantity=order.remaining_quantity,
                        price=bar.close
                    )

            # 统计账户总权益
            total_nav = sum(acc.balance for acc in self.ts.accounts.values())
            self.equity_curve.append(total_nav)


