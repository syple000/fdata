
from .models import *
from .config import *
from .clock import Clock, RClock, VClock
from fdata.dao.csv_dao import CSVGenericDAO

import os
from typing import Dict
import random
from decimal import Decimal
import pandas as pd

CLOCK: Clock = None # 全局时钟，对于vclock支持更新时间

def get_clock() -> Clock:
    if CLOCK is None:
        raise ValueError("Clock has not been initialized. Please call init_vclock or init_rclock first.")
    return CLOCK

def init_rclock():
    global CLOCK
    CLOCK = RClock()

def init_vclock(time: str = '2001-01-01 00:00:00'):
    global CLOCK
    CLOCK = VClock(time=time)

class TradingSystem:
    def __init__(self, account: Account, dividend_infos: Dict[str, pd.DataFrame]):
        self.account = account
        self.dividend_infos = dividend_infos
        self.orders: Dict[str, Order] = {}
        self.trades: Dict[str, Trade] = {}

    def start_day(self):
        date = get_clock().get_date()

        # 将持仓的冻结部分解冻
        for position in self.account.positions.values():
            position.available_quantity += position.frozen_quantity
            position.frozen_quantity = Decimal('0')
            assert position.available_quantity == position.quantity, "可用持仓数量应等于总持仓数量"

        # 遍历账户持仓，进行分红送配股计算
        for symbol, position in self.account.positions.items():
            if symbol in self.dividend_infos:
                dividend_info = self.dividend_infos[symbol]
                # 获取分红信息
                dividend_info = dividend_info[dividend_info['ex_dividend_date'] == date]
                if dividend_info.empty:
                    continue
                dividend_row = dividend_info.iloc[0]

                # 计算分红(10股分红n元)，直接体现到资金与持仓成本
                cash_dividend = Decimal(str(dividend_row['cash_dividend']))
                if cash_dividend > 0:
                    dividend_amount = (position.quantity / Decimal(10)) * cash_dividend
                    self.account.available_balance += dividend_amount
                    self.account.balance += dividend_amount
                    position.cost -= dividend_amount
 
                # 计算送配股(10股送转n股)，直接体现到持仓
                total_transfer_ratio = Decimal(str(dividend_row['total_transfer_ratio']))
                if total_transfer_ratio > 0:
                    dividend_quantity = (position.quantity / Decimal(10)) * total_transfer_ratio
                    position.quantity += dividend_quantity
                    position.available_quantity += dividend_quantity

    def end_day(self, order_dao: CSVGenericDAO[Order], trade_dao: CSVGenericDAO[Trade], pnl_dao: CSVGenericDAO[PNL], current_price: Dict[str, Decimal]): # 关闭所有未结束订单，对订单交易进行数据落地
        orders = []
        for order in self.orders.values():
            if order.status != OrderStatus.FILLED and order.status != OrderStatus.CANCELLED and order.status != OrderStatus.REJECTED:
                self.cancel_order(order.order_id)
            orders.append(order)
        orders.sort(key=lambda x: x.create_time)
        order_dao.write_records(orders)
        self.orders = {}

        trades = []
        for trade in self.trades.values():
            trades.append(trade)
        trades.sort(key=lambda x: x.trade_time)
        trade_dao.write_records(trades)
        self.trades = {}

        # 计算账户市值和盈亏
        for symbol, position in self.account.positions.items():
            if symbol not in current_price:
                raise ValueError(f"当前价格中缺少证券代码: {symbol}")
            market_value = position.get_market_value(current_price[symbol])
            profit_loss = position.get_unrealized_pnl(current_price[symbol])
            pnl_dao.write_record(PNL(
                date=get_clock().get_date(),
                account_id=self.account.account_id,
                symbol=symbol,
                quantity=position.quantity,
                cost=position.cost,
                market_value=market_value,
                profit_loss=profit_loss
            ))

    def submit_order(self, order: Order) -> bool:
        """提交订单"""
        if not self._freeze_assets(order):
            order.status = OrderStatus.REJECTED
            return False
        
        order.create_time = get_clock().get_time()
        order.update_time = get_clock().get_time()
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
        order.update_time = get_clock().get_time()
        return True
    
    def execute_trade(self, order_id: str, quantity: Decimal, price: Decimal) -> Trade:
        """执行成交"""
        order = self.orders[order_id]
        
        # 创建成交记录
        trade = Trade(
            trade_id=f"T{get_clock().get_ts()}{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))}",
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=quantity,
            price=price,
            amount=quantity * price,
            commission=quantity * price * COMMISSION_RATE,
            tax=quantity * price * TAX_RATE if order.side == OrderSide.SELL else Decimal('0'),
            trade_time=get_clock().get_time(),
            account_id=order.account_id
        )

        # 更新订单、持仓和资金
        self._update(order, trade)
        
        trade.status = TradeStatus.CONFIRMED
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
        order.update_time = str(get_clock().get_time())

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
            self.account.positions[trade.symbol].cost += trade.amount + trade.commission + trade.tax

            self.account.frozen_balance = self.account.frozen_balance - (order.price * trade.quantity * (1 + COMMISSION_RATE))
            self.account.available_balance = self.account.available_balance + (order.price * trade.quantity * (1 + COMMISSION_RATE)) - trade.amount - trade.commission - trade.tax
            self.account.balance = self.account.balance - trade.amount - trade.commission  - trade.tax
            assert self.account.available_balance >= 0, "Available balance cannot be negative"
            assert self.account.frozen_balance >= 0, "Frozen balance cannot be negative"
            assert self.account.balance >= 0, "Account balance cannot be negative"
        elif trade.side == OrderSide.SELL:
            self.account.positions[trade.symbol].quantity -= trade.quantity
            self.account.positions[trade.symbol].frozen_quantity -= trade.quantity
            self.account.positions[trade.symbol].cost -= (trade.amount - trade.commission - trade.tax)
            assert self.account.positions[trade.symbol].quantity >= 0, "Position quantity cannot be negative"
            assert self.account.positions[trade.symbol].frozen_quantity >= 0, "Frozen position quantity cannot be negative"

            if self.account.positions[trade.symbol].quantity <= 0:
                pass
                # del self.account.positions[trade.symbol]

            self.account.available_balance = self.account.available_balance + trade.amount - trade.commission - trade.tax
            self.account.balance = self.account.balance + trade.amount - trade.commission - trade.tax
        else:
            raise Exception(f"Unknown order side: {trade.side}")

if __name__ == '__main__':
    init_vclock(time='2023-10-01 09:30:00')

    # 测试的印花税5/万，手续费1/万
    # 初始化账户：2w 资金，持仓 000001.SZ 200 股
    account = Account(
        account_id='ACC1',
        balance=Decimal('19968'),
        available_balance=Decimal('19968'),
        frozen_balance=Decimal('0'),
        positions={}
    )
    account.positions['000001.SZ'] = Position(
        symbol='000001.SZ',
        quantity=Decimal('160'),
        available_quantity=Decimal('140'),
        frozen_quantity=Decimal('20'),
        cost=Decimal('2032')
    )
    ts = TradingSystem(account, {'000001.SZ': pd.DataFrame({
        'ex_dividend_date': ['2023-10-01'],
        'total_transfer_ratio': [2.5],
        'cash_dividend': [2],
    })})
    ts.start_day()  # 开始交易日


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
        assert_equal(account.positions['000001.SZ'].cost, Decimal('2000'), "持仓成本") 
        trade_b = ts.execute_trade(order_b1.order_id, quantity=Decimal('100'), price=Decimal('10'))
        assert order_b1.status == OrderStatus.FILLED, f"Expected order status FILLED, got {order_b1.status}"
        assert_equal(trade_b.quantity, Decimal('100'), "成交数量")
        assert_equal(trade_b.price, Decimal('10'), "成交价格")
        assert_equal(account.balance, Decimal('18999.9'), "余额")
        assert_equal(account.available_balance, Decimal('18999.9'), "可用余额")
        assert_equal(account.frozen_balance, Decimal('0'), "冻结余额")
        pos = account.positions['000001.SZ']
        assert_equal(pos.cost, Decimal('3000.1'), "持仓成本")
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
        assert_equal(pos.cost, Decimal('2300.52'), "持仓成本")
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
    assert_equal(pos2.cost, Decimal('360.036'), "持仓成本")
    assert_equal(pos2.quantity, Decimal('20'), "持仓总量")
    assert_equal(pos2.frozen_quantity, Decimal('20'), "冻结持仓")
    assert_equal(pos2.available_quantity, Decimal('0'), "可用持仓")

    # 测试挂单，交易日结束自动取消
    order_b4 = Order(
        order_id='B4',
        account_id='ACC1',
        symbol='000002.SZ',
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal('50'),
        price=Decimal('20'),
        status=None,
        filled_quantity=Decimal('0'),
        remaining_quantity=Decimal('50'),
    )
    ts.submit_order(order_b4)

    current_price = {
        '000001.SZ': Decimal('12'),
        '000002.SZ': Decimal('19')
    }
    if os.path.exists('orders.csv'):
        os.remove('orders.csv')
    if os.path.exists('trades.csv'):
        os.remove('trades.csv')
    if os.path.exists('pnl.csv'):
        os.remove('pnl.csv')
    with CSVGenericDAO[Order]('orders.csv', Order) as order_dao, \
         CSVGenericDAO[Trade]('trades.csv', Trade) as trade_dao, \
         CSVGenericDAO[PNL]('pnl.csv', PNL) as pnl_dao:
        ts.end_day(order_dao, trade_dao, pnl_dao, current_price)

    print(ts.account)
    print(f'market_value: {ts.account.get_market_value(current_price)}')
    print(f'total_asset: {ts.account.get_total_asset(current_price)}')
    print(f'profit_loss: {ts.account.get_profit_loss(current_price)}')
    for pos in ts.account.positions.values():
        print(f'{pos.symbol} - 市值: {pos.get_market_value(current_price[pos.symbol])}, '
              f'盈亏: {pos.get_unrealized_pnl(current_price[pos.symbol])}')