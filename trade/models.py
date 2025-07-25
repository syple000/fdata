from decimal import Decimal
from enum import Enum
from typing import Optional, Dict
from dataclasses import dataclass, field
import random
from datetime import datetime

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
        
        # 赋值时间/剩余quantity
        if self.create_time is None:
            self.create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if self.update_time is None:
            self.update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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

        # 赋值时间
        if self.trade_time is None:
            self.trade_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
   
@dataclass
class Position:
    """持仓模型"""
    symbol: str
    quantity: Decimal  # 持仓数量
    available_quantity: Decimal  # 可用数量
    frozen_quantity: Decimal = Decimal('0')  # 冻结数量

@dataclass
class Account:
    """资金账户模型"""
    account_id: str
    balance: Decimal = Decimal('0') # 账户余额
    available_balance: Decimal  = Decimal('0') # 可用余额
    frozen_balance: Decimal = Decimal('0')  # 冻结资金

    market_value: Decimal = Decimal('0')  # 持仓市值
    total_asset: Decimal = Decimal('0')  # 总资产
    profit_loss: Decimal = Decimal('0')  # 盈亏

    commission_total: Decimal = Decimal('0')  # 累计手续费
    tax_total: Decimal = Decimal('0')  # 累计税费

    positions: Dict[str, Position] = field(default_factory=dict)