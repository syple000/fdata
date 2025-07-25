
from .models import *

from typing import Dict
from datetime import datetime
import random
from decimal import Decimal

# 定义证券交易：订单、成交、资金账户
COMMISSION_RATE = Decimal('0.0001')  # 手续费率
TAX_RATE = Decimal('0.0005')  # 印花税率

class TradingSystem:
    def __init__(self, account: Account):
        self.account = account
        self.orders: Dict[str, Order] = {}
        self.trades: Dict[str, Trade] = {}

    def submit_order(self, order: Order) -> bool:
        """提交订单"""
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
        order.update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return True
    
    def execute_trade(self, order_id: str, quantity: Decimal, price: Decimal) -> Trade:
        """执行成交"""
        order = self.orders[order_id]
        
        # 创建成交记录
        trade = Trade(
            trade_id=f"T{datetime.now().strftime('%Y%m%d%H%M%S')}{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))}",
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=quantity,
            price=price,
            amount=quantity * price,
            commission=quantity * price * COMMISSION_RATE,
            tax=quantity * price * TAX_RATE if order.side == OrderSide.SELL else Decimal('0'),
            account_id=order.account_id
        )

        # 更新订单、持仓和资金
        self._update(order, trade)
        
        self.trades[trade.trade_id] = trade
        return trade
    
    def _freeze_assets(self, order: Order) -> bool:
        if order.side == OrderSide.BUY:
            required_funds = order.remaining_quantity * order.price * (1 + COMMISSION_RATE)
            if self.account.available_balance < required_funds:
                return False
            self.account.available_balance -= required_funds
            self.account.frozen_balance += required_funds
            return True
        elif order.side == OrderSide.SELL:
            required_quantity = order.remaining_quantity
            if order.symbol not in self.account.positions or \
               self.account.positions[order.symbol].available_quantity < required_quantity:
                return False
            self.account.positions[order.symbol].available_quantity -= order.quantity
            self.account.positions[order.symbol].frozen_quantity += order.quantity
            return True
        else:
            raise Exception(f"Unknown order side: {order.side}")
    
    def _unfreeze_assets(self, order: Order):
        """解冻资产"""
        if order.side == OrderSide.BUY:
            required_funds = order.remaining_quantity * order.price * (1 + COMMISSION_RATE)
            assert self.account.frozen_balance >= required_funds, "Frozen balance cannot be less than required funds"
            self.account.frozen_balance -= required_funds
            self.account.available_balance += required_funds
        elif order.side == OrderSide.SELL:
            required_quantity = order.remaining_quantity
            assert order.symbol in self.account.positions, "Position must exist for sell orders"
            assert self.account.positions[order.symbol].frozen_quantity >= required_quantity, "Frozen quantity cannot be less than required quantity"
            self.account.positions[order.symbol].frozen_quantity -= required_quantity
            self.account.positions[order.symbol].available_quantity += required_quantity
        else:
            raise Exception(f"Unknown order side: {order.side}")
    
    def _update(self, order: Order, trade: Trade):
        # 更新订单状态
        assert order.remaining_quantity >= trade.quantity, "Trade quantity cannot exceed remaining order quantity"
        order.filled_quantity += trade.quantity
        order.remaining_quantity -= trade.quantity
        if order.remaining_quantity <= 0:
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.PARTIALLY_FILLED
        order.update_time = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # 更新账户数据
        if trade.side == OrderSide.BUY:
            if trade.symbol not in self.account.positions:
                self.account.positions[trade.symbol] = Position(
                    symbol=trade.symbol,
                    quantity=0,
                    available_quantity=0,
                    frozen_quantity=0
                )

            self.account.positions[trade.symbol].quantity += trade.quantity
            self.account.positions[trade.symbol].frozen_quantity += trade.quantity

            self.account.frozen_balance = self.account.frozen_balance - (order.price * trade.quantity * (1 + COMMISSION_RATE))
            self.account.available_balance = self.account.available_balance + (order.price * trade.quantity * (1 + COMMISSION_RATE)) - trade.amount - trade.commission
            self.account.balance = self.account.balance - trade.amount - trade.commission 
            assert self.account.available_balance >= 0, "Available balance cannot be negative"
            assert self.account.frozen_balance >= 0, "Frozen balance cannot be negative"
            assert self.account.balance >= 0, "Account balance cannot be negative"
        elif trade.side == OrderSide.SELL:
            self.account.positions[trade.symbol].quantity -= trade.quantity
            self.account.positions[trade.symbol].frozen_quantity -= trade.quantity
            assert self.account.positions[trade.symbol].quantity >= 0, "Position quantity cannot be negative"
            assert self.account.positions[trade.symbol].frozen_quantity >= 0, "Frozen position quantity cannot be negative"

            if self.account.positions[trade.symbol].quantity <= 0:
                del self.account.positions[trade.symbol]

            self.account.available_balance = self.account.available_balance + trade.amount - trade.commission - trade.tax
            self.account.balance = self.account.balance + trade.amount - trade.commission - trade.tax
        else:
            raise Exception(f"Unknown order side: {trade.side}")

if __name__ == '__main__':
    # 测试的印花税5/万，手续费1/万
    # 初始化账户：2w 资金，持仓 000001.SZ 200 股
    account = Account(
        account_id='ACC1',
        balance=Decimal('20000'),
        available_balance=Decimal('20000'),
        frozen_balance=Decimal('0'),
        positions={}
    )
    account.positions['000001.SZ'] = Position(
        symbol='000001.SZ',
        quantity=Decimal('200'),
        available_quantity=Decimal('200'),
        frozen_quantity=Decimal('0')
    )
    ts = TradingSystem(account)

    def assert_equal(actual, expected, msg_prefix=""):
        assert actual == expected, f"{msg_prefix}期望值{expected}，实际值{actual}，差异{abs(actual - expected)}"

    # 初始化账号状态断言
    assert_equal(account.balance, Decimal('20000'), "余额")
    assert_equal(account.available_balance, Decimal('20000'), "可用余额")
    assert_equal(account.frozen_balance, Decimal('0'), "冻结余额")

    pos = account.positions.get('000001.SZ')
    assert pos is not None, "应存在000001.SZ持仓"
    assert_equal(pos.quantity, Decimal('200'), "持仓总量")
    assert_equal(pos.available_quantity, Decimal('200'), "可用持仓")
    assert_equal(pos.frozen_quantity, Decimal('0'), "冻结持仓")

    # 1) 买入 100 股 @ 10，预期成功
    order_b1 = Order(
        order_id='B1',
        account_id='ACC1',
        symbol='000001.SZ',
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal('100'),
        price=Decimal('10'),
        status=None,
        filled_quantity=Decimal('0'),
        remaining_quantity=Decimal('100'),
        update_time=None
    )
    ok = ts.submit_order(order_b1)
    assert ok, f"Expected order submission success, got {ok}"
    assert order_b1.status == OrderStatus.SUBMITTED, f"Expected SUBMITTED, got {order_b1.status}"
    assert_equal(account.available_balance, Decimal('18999.9'), "可用余额")
    assert_equal(account.frozen_balance, Decimal('1000.1'), "冻结余额")
    pos = account.positions['000001.SZ']
    assert_equal(pos.quantity, Decimal('200'), "持仓总量")
    assert_equal(pos.available_quantity, Decimal('200'), "可用持仓")
    assert_equal(pos.frozen_quantity, Decimal('0'), "冻结持仓")

    # 2) 买入 3000 股 @ 100，预期失败（资金不足）
    order_b2 = Order(
        order_id='B2',
        account_id='ACC1',
        symbol='000001.SZ',
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal('3000'),
        price=Decimal('100'),
        status=None,
        filled_quantity=Decimal('0'),
        remaining_quantity=Decimal('3000'),
        update_time=None
    )
    ok2 = ts.submit_order(order_b2)
    assert not ok2, f"Expected order submission failure due to insufficient funds, got {ok2}"
    assert order_b2.status == OrderStatus.REJECTED, f"Expected status REJECTED, got {order_b2.status}"
    assert_equal(account.available_balance, Decimal('18999.9'), "可用余额")
    assert_equal(account.frozen_balance, Decimal('1000.1'), "冻结余额")
    pos = account.positions['000001.SZ']
    assert_equal(pos.quantity, Decimal('200'), "持仓总量")
    assert_equal(pos.available_quantity, Decimal('200'), "可用持仓")
    assert_equal(pos.frozen_quantity, Decimal('0'), "冻结持仓")

    # 3) 卖出 50 股 @ 15，预期成功
    order_s1 = Order(
        order_id='S1',
        account_id='ACC1',
        symbol='000001.SZ',
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=Decimal('50'),
        price=Decimal('15'),
        status=None,
        filled_quantity=Decimal('0'),
        remaining_quantity=Decimal('50'),
        update_time=None
    )
    ok3 = ts.submit_order(order_s1)
    assert ok3, f"Expected SELL 50@15 submission success, got {ok3}"
    assert order_s1.status == OrderStatus.SUBMITTED, f"Expected status SUBMITTED, got {order_s1.status}"
    # 资金不变
    assert_equal(account.available_balance, Decimal("18999.9"), "可用余额")
    assert_equal(account.frozen_balance, Decimal("1000.1"), "冻结余额")
    # 持仓冻结 50 股
    pos = account.positions['000001.SZ']
    assert_equal(pos.quantity, Decimal("200"), "持仓总量")
    assert_equal(pos.available_quantity, Decimal("150"), "可用持仓")
    assert_equal(pos.frozen_quantity, Decimal("50"), "冻结持仓")

    # 4) 卖出 1000 股 @ 15，预期失败（持仓不足）
    order_s2 = Order(
        order_id='S2',
        account_id='ACC1',
        symbol='000001.SZ',
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=Decimal('1000'),
        price=Decimal('15'),
        status=None,
        filled_quantity=Decimal('0'),
        remaining_quantity=Decimal('1000'),
        update_time=None
    )
    ok4 = ts.submit_order(order_s2)
    assert not ok4, f"Expected order submission failure due to insufficient holdings, got {ok4}"
    assert order_s2.status == OrderStatus.REJECTED, f"Expected status REJECTED, got {order_s2.status}"
    assert_equal(account.available_balance, Decimal('18999.9'), "可用余额")
    assert_equal(account.frozen_balance, Decimal('1000.1'), "冻结余额")
    pos = account.positions['000001.SZ']
    assert_equal(pos.quantity, Decimal('200'), "持仓总量")
    assert_equal(pos.available_quantity, Decimal('150'), "可用持仓")
    assert_equal(pos.frozen_quantity, Decimal('50'), "冻结持仓")

    # 5) 执行买单成交
    if order_b1.status == OrderStatus.SUBMITTED:
        trade_b = ts.execute_trade(order_b1.order_id, quantity=Decimal('100'), price=Decimal('10'))
        assert order_b1.status == OrderStatus.FILLED, f"Expected order status FILLED, got {order_b1.status}"
        assert_equal(trade_b.quantity, Decimal('100'), "成交数量")
        assert_equal(trade_b.price, Decimal('10'), "成交价格")
        assert_equal(account.balance, Decimal('18999.9'), "余额")
        assert_equal(account.available_balance, Decimal('18999.9'), "可用余额")
        assert_equal(account.frozen_balance, Decimal('0'), "冻结余额")
        pos = account.positions['000001.SZ']
        assert_equal(pos.quantity, Decimal('300'), "持仓总量")
        assert_equal(pos.available_quantity, Decimal('150'), "可用持仓")
        assert_equal(pos.frozen_quantity, Decimal('150'), "冻结持仓")

    # 6) 执行卖单成交
    if order_s1.status == OrderStatus.SUBMITTED:
        trade_s = ts.execute_trade(order_s1.order_id, quantity=Decimal('50'), price=Decimal('14'))
        assert order_s1.status == OrderStatus.FILLED, f"Expected SELL order status FILLED, got {order_s1.status}"
        assert_equal(trade_s.quantity, Decimal('50'), "成交数量")
        assert_equal(trade_s.price, Decimal('14'), "成交价格")
        assert_equal(account.balance, Decimal('19699.48'), "余额")
        assert_equal(account.available_balance, Decimal('19699.48'), "可用余额")
        assert_equal(account.frozen_balance, Decimal('0'), "冻结余额")

        pos = account.positions['000001.SZ']
        assert_equal(pos.quantity, Decimal('250'), "持仓总量")
        assert_equal(pos.available_quantity, Decimal('150'), "可用持仓")
        assert_equal(pos.frozen_quantity, Decimal('100'), "冻结持仓")

    # 7) 测试000002.SZ股票买卖部分成交并撤单
    order_b3 = Order(
        order_id='B3',
        account_id='ACC1',
        symbol='000002.SZ',
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal('50'),
        price=Decimal('20'),
        status=None,
        filled_quantity=Decimal('0'),
        remaining_quantity=Decimal('50'),
        update_time=None
    )
    # 提交订单
    ok5 = ts.submit_order(order_b3)
    assert ok5, f"Expected BUY submission success, got {ok5}"
    assert order_b3.status == OrderStatus.SUBMITTED, f"Expected SUBMITTED, got {order_b3.status}"
    assert_equal(account.frozen_balance, Decimal('1000.1'), "冻结余额")
    assert_equal(account.balance, Decimal('19699.48'), "余额")
    assert_equal(account.available_balance, Decimal('18699.38'), "可用余额")
    # 部分成交 20 股
    trade_b3 = ts.execute_trade(order_b3.order_id, quantity=Decimal('20'), price=Decimal('18'))
    assert order_b3.status == OrderStatus.PARTIALLY_FILLED, f"Expected PARTIALLY_FILLED, got {order_b3.status}"
    assert_equal(trade_b3.quantity, Decimal('20'), "成交数量")
    assert_equal(trade_b3.price, Decimal('18'), "成交价格")
    # 检查成交后资金变化
    assert_equal(account.balance, Decimal('19339.444'), "余额")
    assert_equal(account.frozen_balance, Decimal('600.06'), "冻结余额")
    assert_equal(account.available_balance, Decimal('18739.384'), "可用余额")
    # 撤单
    ok_cancel = ts.cancel_order(order_b3.order_id)
    assert ok_cancel, "Expected cancel success"
    assert order_b3.status == OrderStatus.CANCELLED, f"Expected CANCELLED, got {order_b3.status}"
    # 撤单后冻结资金归零，可用资金恢复
    assert_equal(account.frozen_balance, Decimal('0'), "冻结余额")
    assert_equal(account.available_balance, Decimal('19339.444'), "可用余额")
    assert_equal(account.balance, Decimal('19339.444'), "余额")
    # 持仓情况：已成交 20 股，冻结持仓 20
    pos2 = account.positions['000002.SZ']
    assert_equal(pos2.quantity, Decimal('20'), "持仓总量")
    assert_equal(pos2.frozen_quantity, Decimal('20'), "冻结持仓")
    assert_equal(pos2.available_quantity, Decimal('0'), "可用持仓")