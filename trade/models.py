from decimal import Decimal
from enum import Enum
from typing import Optional
from dataclasses import dataclass

# 定义证券交易：订单、成交、资金账户

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
    symbol: str  # 证券代码
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Optional[Decimal] = None  # 市价单可为空
    stop_price: Optional[Decimal] = None  # 止损价
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: Decimal = Decimal('0')
    remaining_quantity: Decimal = None
    create_time: str = None
    update_time: str = None
    account_id: str = None
   
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
    commission: Decimal = Decimal('0')  # 手续费
    tax: Decimal = Decimal('0')  # 印花税
    status: TradeStatus = TradeStatus.PENDING
    trade_time: str = None
    settle_time: Optional[str] = None
    account_id: str = None

@dataclass
class Position:
    """持仓模型"""
    symbol: str
    quantity: Decimal  # 持仓数量
    available_quantity: Decimal  # 可用数量
    frozen_quantity: Decimal = Decimal('0')  # 冻结数量
    cost_price: Decimal = Decimal('0')  # 成本价
    market_price: Decimal = Decimal('0')  # 市价
    unrealized_pnl: Decimal = Decimal('0')  # 浮动盈亏
    realized_pnl: Decimal = Decimal('0')  # 已实现盈亏
    update_time: str = None

@dataclass
class Account:
    """资金账户模型"""
    account_id: str
    balance: Decimal  # 账户余额
    available_balance: Decimal  # 可用余额
    frozen_balance: Decimal = Decimal('0')  # 冻结资金
    market_value: Decimal = Decimal('0')  # 持仓市值
    total_asset: Decimal = Decimal('0')  # 总资产
    profit_loss: Decimal = Decimal('0')  # 盈亏
    commission_total: Decimal = Decimal('0')  # 累计手续费
    tax_total: Decimal = Decimal('0')  # 累计税费
    create_time: str = None
    update_time: str = None

class TradingSystem:
    """交易系统"""
    
    def __init__(self):
        self.orders = {}
        self.trades = {}
        self.accounts = {}
        self.positions = {}
    
    def submit_order(self, order: Order) -> bool:
        """提交订单"""
        # 风控检查
        if not self._risk_check(order):
            order.status = OrderStatus.REJECTED
            return False
        
        # 冻结资金/股票
        if not self._freeze_assets(order):
            order.status = OrderStatus.REJECTED
            return False
        
        order.status = OrderStatus.SUBMITTED
        self.orders[order.order_id] = order
        return True
    
    def cancel_order(self, order_id: str) -> bool:
        """撤销订单"""
        if order_id not in self.orders:
            return False
        
        order = self.orders[order_id]
        if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]:
            return False
        
        # 解冻资金/股票
        self._unfreeze_assets(order)
        order.status = OrderStatus.CANCELLED
        order.update_time = str.now()
        return True
    
    def execute_trade(self, order_id: str, quantity: Decimal, price: Decimal) -> Trade:
        """执行成交"""
        order = self.orders[order_id]
        
        # 创建成交记录
        trade = Trade(
            trade_id=f"T{str.now().strftime('%Y%m%d%H%M%S')}",
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=quantity,
            price=price,
            amount=quantity * price,
            account_id=order.account_id
        )
        
        # 更新订单状态
        order.filled_quantity += quantity
        order.remaining_quantity -= quantity
        
        if order.remaining_quantity <= 0:
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.PARTIALLY_FILLED
        
        order.update_time = str.now()
        
        # 更新持仓和资金
        self._update_position(trade)
        self._update_account(trade)
        
        self.trades[trade.trade_id] = trade
        return trade
    
    def _risk_check(self, order: Order) -> bool:
        """风控检查"""
        # 实现具体的风控逻辑
        return True
    
    def _freeze_assets(self, order: Order) -> bool:
        """冻结资产"""
        # 实现资金/股票冻结逻辑
        return True
    
    def _unfreeze_assets(self, order: Order):
        """解冻资产"""
        # 实现资产解冻逻辑
        pass
    
    def _update_position(self, trade: Trade):
        """更新持仓"""
        # 实现持仓更新逻辑
        pass
    
    def _update_account(self, trade: Trade):
        """更新账户"""
        # 实现账户更新逻辑
        pass