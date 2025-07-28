from decimal import Decimal
from enum import Enum
from typing import Optional, Dict
from dataclasses import dataclass, field
import pandas as pd

from .config import *

class OrderType(Enum):
    """订单类型"""
    MARKET = "市价单"
    LIMIT = "限价单"
    STOP = "止损单"
    STOP_LIMIT = "止损限价单"

    def to_string(self):
        """返回订单类型的字符串表示"""
        return self.value
    
    @staticmethod
    def from_string(type_str: str):
        """从字符串转换为订单类型"""
        for order_type in OrderType:
            if order_type.value == type_str:
                return order_type
        raise ValueError(f"Unknown order type: {type_str}")


class OrderSide(Enum):
    """买卖方向"""
    BUY = "买入"
    SELL = "卖出"

    def to_string(self):
        """返回买卖方向的字符串表示"""
        return self.value
    
    @staticmethod
    def from_string(side_str: str):
        """从字符串转换为买卖方向"""
        for order_side in OrderSide:
            if order_side.value == side_str:
                return order_side
        raise ValueError(f"Unknown order side: {side_str}")


class OrderStatus(Enum):
    """订单状态"""
    PENDING = "待提交"
    SUBMITTED = "已提交"
    PARTIALLY_FILLED = "部分成交"
    FILLED = "全部成交"
    CANCELLED = "已撤销"
    REJECTED = "已拒绝"

    def to_string(self):
        """返回订单状态的字符串表示"""
        return self.value
    
    @staticmethod
    def from_string(status_str: str):
        """从字符串转换为订单状态"""
        for order_status in OrderStatus:
            if order_status.value == status_str:
                return order_status
        raise ValueError(f"Unknown order status: {status_str}")

class TradeStatus(Enum):
    """成交状态"""
    PENDING = "待确认"
    CONFIRMED = "已确认"
    SETTLED = "已结算"

    def to_string(self):
        """返回成交状态的字符串表示"""
        return self.value
    
    @staticmethod
    def from_string(status_str: str):
        """从字符串转换为成交状态"""
        for trade_status in TradeStatus:
            if trade_status.value == status_str:
                return trade_status
        raise ValueError(f"Unknown trade status: {status_str}")


@dataclass
class Order:
    """订单模型"""
    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType = OrderType.LIMIT
    quantity: Decimal = None
    price: Optional[Decimal] = None  # 市价单可为空
    stop_price: Optional[Decimal] = None  # 止损价
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: Decimal = Decimal('0')
    remaining_quantity: Decimal = None
    create_time: str = None
    update_time: str = None
    account_id: str = None

    def __post_init__(self):
        # 检查订单
        if self.order_type in [OrderType.MARKET, OrderType.STOP, OrderType.STOP_LIMIT]:
            raise ValueError(f"暂不支持订单类型: {self.order_type}")
        if self.order_type in [OrderType.LIMIT] and self.price is None:
            raise ValueError("限价单必须包含限价")

        if self.price is not None and self.price <= 0:
            raise ValueError("价格必须大于0")
        if self.stop_price is not None and self.stop_price <= 0:
            raise ValueError("止损价必须大于0")
        if self.quantity <= 0:
            raise ValueError("数量必须大于0")
        if len(self.order_id) <= 0:
            raise ValueError("订单ID不能为空")
        if len(self.symbol) <= 0:
            raise ValueError("证券代码不能为空")
        if len(self.account_id) <= 0:
            raise ValueError("账户ID不能为空")
        
        self.remaining_quantity = self.quantity
    
@dataclass
class Trade:
    """成交记录模型"""
    trade_id: str
    order_id: str
    symbol: str
    side: OrderSide
    quantity: Decimal
    price: Decimal
    amount: Decimal  # 成交金额
    commission: Decimal  # 手续费
    tax: Decimal  # 印花税
    status: TradeStatus = TradeStatus.PENDING
    trade_time: str = None
    account_id: str = None

    def __post_init__(self):
        # 检查成交记录
        if self.quantity <= 0:
            raise ValueError("成交数量必须大于0")
        if self.price <= 0:
            raise ValueError("成交价格必须大于0")
        if self.amount <= 0:
            raise ValueError("成交金额必须大于0")
        if len(self.trade_id) <= 0:
            raise ValueError("成交ID不能为空")
        if len(self.order_id) <= 0:
            raise ValueError("订单ID不能为空")
        if len(self.symbol) <= 0:
            raise ValueError("证券代码不能为空")
        if len(self.account_id) <= 0:
            raise ValueError("账户ID不能为空")
   
@dataclass
class Position:
    """持仓模型"""
    symbol: str
    quantity: Decimal              # 持仓数量
    available_quantity: Decimal    # 可用数量
    frozen_quantity: Decimal = Decimal('0')  # 冻结数量

    cost: Decimal = Decimal('0')   # 持仓成本

    def get_market_value(self, current_price: Decimal) -> Decimal:
        """计算持仓市值"""
        return self.quantity * current_price

    def get_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """计算未实现盈亏"""
        return current_price * self.quantity * (1 - COMMISSION_RATE - TAX_RATE) - self.cost

    def get_unrealized_pnl_rate(self, current_price: Decimal) -> Decimal:
        """计算未实现盈亏率"""
        if self.cost == 0:
            return Decimal('0')
        unrealized_pnl = self.get_unrealized_pnl(current_price)
        return unrealized_pnl / self.cost


@dataclass
class Account:
    """资金账户模型"""
    account_id: str
    balance: Decimal = Decimal('0') # 账户余额
    available_balance: Decimal  = Decimal('0') # 可用余额
    frozen_balance: Decimal = Decimal('0')  # 冻结资金

    positions: Dict[str, Position] = field(default_factory=dict)

    def get_market_value(self, current_price: Dict[str, Decimal]) -> Decimal:
        """计算账户持仓市值"""
        sum_market_value = Decimal('0')
        for symbol, position in self.positions.items():
            if symbol not in current_price:
                raise ValueError(f"当前价格中缺少证券代码: {symbol}")
            sum_market_value += position.get_market_value(current_price[symbol])
        return sum_market_value

    def get_total_asset(self, current_price: Dict[str, Decimal]) -> Decimal:
        """计算账户总资产"""
        market_value = self.get_market_value(current_price)
        total_asset = self.balance + market_value
        return total_asset

    def get_profit_loss(self, current_price: Dict[str, Decimal]) -> Decimal:
        """计算账户盈亏"""
        profit_loss = Decimal('0')
        for symbol, position in self.positions.items():
            if symbol not in current_price:
                raise ValueError(f"当前价格中缺少证券代码: {symbol}")
            profit_loss += position.get_unrealized_pnl(current_price[symbol])
        return profit_loss

@dataclass
class PNL:
    date: str
    account_id: str
    symbol: str
    quantity: Decimal
    cost: Decimal
    market_value: Decimal
    profit_loss: Decimal

@dataclass
class Bar:
    """K线数据模型"""
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    start_timestamp: str  # 开始时间戳，格式为 'YYYY-MM-DD HH:MM:SS'
    end_timestamp: str  # 结束时间戳，格式为 'YYYY-MM-DD HH:MM:SS'

    def __post_init__(self):
        if self.open <= 0 or self.high <= 0 or self.low <= 0 or self.close <= 0:
            raise ValueError("开盘价、最高价、最低价和收盘价必须大于0")
        if self.volume < 0:
            raise ValueError("成交量不能为负数")
        if len(self.symbol) <= 0:
            raise ValueError("证券代码不能为空")
        if len(self.start_timestamp) <= 0:
            raise ValueError("开始时间戳不能为空")
        if len(self.end_timestamp) <= 0:
            raise ValueError("结束时间戳不能为空")

@dataclass
class TargetPosition: # 目标持仓信号
    symbol: str
    quantity: Decimal

@dataclass
class Fundamental:
    symbol: str
    financial_data: pd.DataFrame # 对齐FinancialData结构
    dividend_info: pd.DataFrame # 对齐DividendInfo结构
    kline_data: pd.DataFrame # 对齐HistoricalData结构
    forward_adjusted_kline_data: pd.DataFrame # 对齐HistoricalData结构

@dataclass
class MarketSnapshot:
    date: str
    symbols: Dict[str, Fundamental]