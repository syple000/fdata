from dataclasses import dataclass
from datetime import datetime
from contextlib import ExitStack, AsyncExitStack
from decimal import Decimal 
import pandas as pd
import os
import logging

from fdata.dao.csv_dao import CSVGenericDAO
from fdata.market_data.models import KLineType
from fdata.utils.rand_str import rand_str
from .models import Order, Trade, Bar, OrderStatus, TradeStatus, Account, PNL, OrderSide
from .trading_system import TradingSystem, init_vclock, get_clock 
from .strategy import Strategy, TestStrategy
from .data_feed import BacktestDataFeed, parse_ts

# 策略回放
class Backtest:
    def __init__(self, ts: TradingSystem, strategy: Strategy, feed: BacktestDataFeed):
        self._ts = ts
        self._strategy = strategy
        self._feed = feed

    def run(self, orders_path: str = 'orders.csv', trades_path: str = 'trades.csv', pnl_path: str = 'pnl.csv'):
        with ExitStack() as stack:
            orders_csv = stack.enter_context(CSVGenericDAO(orders_path, Order))
            trades_csv = stack.enter_context(CSVGenericDAO(trades_path, Trade))
            pnl_csv = stack.enter_context(CSVGenericDAO(pnl_path, PNL))

            cur_day = None
            cur_price = {}
            target_positions = []

            for data in self._feed:
                date = datetime.fromtimestamp(parse_ts(data['date'])).strftime('%Y-%m-%d')
                date_time = datetime.fromtimestamp(parse_ts(data['date'])).strftime('%Y-%m-%d %H:%M:%S')
                symbol_data = data['symbol_data']

                if cur_day is None or cur_day != date:
                    self._ts.end_day(orders_csv, trades_csv, pnl_csv, cur_price)
                    get_clock().set_time(date_time)
                    self._ts.start_day()
                    self._strategy.on_fundamentals(symbol_data) # kline的时间是区间结束的时间（日线是XX日结束；分钟线是XX时间结束），所以这个数据包含了一条未来信息，处理时请注意
                else:
                    get_clock().set_time(date_time)

                cur_day = date

                # 执行上一个bar周期产生的仓位信号
                for target_position in target_positions:
                    logging.info(f"Executing target position for {target_position.symbol} at {date_time}, quantity: {target_position.quantity}")
                    symbol = target_position.symbol
                    quantity = target_position.quantity
                    org_quantity = Decimal('0')
                    if symbol in self._ts.account.positions:
                        org_quantity = self._ts.account.positions[symbol].quantity
                    last_kline = symbol_data[symbol]['forward_adjusted_kline_data'].iloc[-1] # 获取最新的前复权K线数据，不能为空！
                    # 默认以开盘价成交
                    if quantity > org_quantity:
                        order = Order(
                            order_id=f"{parse_ts(date_time)}{rand_str()}",
                            symbol=symbol,
                            side=OrderSide.BUY,
                            quantity=quantity - org_quantity,
                            price=Decimal(last_kline['open_price']),
                            status=OrderStatus.PENDING,
                            account_id=self._ts.account.account_id,
                        )
                        if self._ts.submit_order(order):
                            self._ts.execute_trade(order.order_id, order.quantity, order.price)
                        else:
                            logging.error(f"Failed to submit buy order for {symbol} at {date_time}, target_quantity: {quantity}")
                    elif quantity < org_quantity:
                        order = Order(
                            order_id=f"{parse_ts(date_time)}{rand_str()}",
                            symbol=symbol,
                            side=OrderSide.SELL,
                            quantity=org_quantity - quantity,
                            price=Decimal(last_kline['open_price']),
                            status=OrderStatus.PENDING,
                            account_id=self._ts.account.account_id,
                        )
                        if self._ts.submit_order(order):
                            self._ts.execute_trade(order.order_id, order.quantity, order.price)
                        else:
                            logging.error(f"Failed to submit sell order for {symbol} at {date_time}, target_quantity: {quantity}")
                    else:
                        # 持平，不操作
                        pass

                bars = []
                for symbol, data_map in symbol_data.items():
                    if data_map['forward_adjusted_kline_data'].empty:
                        continue
                    last_kline = data_map['forward_adjusted_kline_data'].iloc[-1]
                    cur_price[symbol] = Decimal(last_kline['close_price'])

                    if self._feed._kline_type == KLineType.DAILY:
                        start_timestamp = last_kline['date']
                        end_timestamp = last_kline['date']
                    elif self._feed._kline_type == KLineType.MIN5:
                        end_timestamp = last_kline['date']
                        start_timestamp = datetime.fromtimestamp(parse_ts(last_kline['date']) - 5 * 60).strftime('%Y-%m-%d %H:%M:%S')
                    elif self._feed._kline_type == KLineType.MIN15:
                        end_timestamp = last_kline['date']
                        start_timestamp = datetime.fromtimestamp(parse_ts(last_kline['date']) - 15 * 60).strftime('%Y-%m-%d %H:%M:%S')
                    elif self._feed._kline_type == KLineType.MIN30:
                        end_timestamp = last_kline['date']
                        start_timestamp = datetime.fromtimestamp(parse_ts(last_kline['date']) - 30 * 60).strftime('%Y-%m-%d %H:%M:%S')
                    elif self._feed._kline_type == KLineType.MIN60:
                        end_timestamp = last_kline['date']
                        start_timestamp = datetime.fromtimestamp(parse_ts(last_kline['date']) - 60 * 60).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        raise ValueError(f"Unsupported kline type: {self._feed._kline_type}")
                    # 创建Bar对象
                    bar = Bar(
                        symbol=symbol,
                        open=Decimal(last_kline['open_price']),
                        high=Decimal(last_kline['high_price']),
                        low=Decimal(last_kline['low_price']),
                        close=Decimal(last_kline['close_price']),
                        volume=Decimal(last_kline['volume']),
                        start_timestamp=start_timestamp,
                        end_timestamp=end_timestamp
                    )
                    bars.append(bar)
                target_positions = self._strategy.on_universe(bars)
                logging.info(f"Processed data for {date} at {date_time}")

            # 退出清理，结束最后一个交易日
            self._ts.end_day(orders_csv, trades_csv, pnl_csv, cur_price)
                        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    df = pd.read_csv('archive/stock_list_沪深300.csv', dtype=str)
    symbols = df['symbol'].tolist()
    symbols = ['000001.SZ', '000002.SZ']

    init_vclock(time='2020-10-01 00:00:00')

    account = Account(
        account_id='ACC1',
        balance=Decimal('100000'),
        available_balance=Decimal('100000'),
        frozen_balance=Decimal('0'),
        positions={}
    )

    dividend_infos = {}
    for symbol in symbols:
        dividend_infos[symbol] = pd.read_csv(f'archive/{symbol}/dividend_info.csv', dtype=str)

    ts = TradingSystem(account, dividend_infos)
    strategy = TestStrategy(account)
    feed = BacktestDataFeed(
        start_date='2015-06-08',
        end_date='2018-06-08',
        symbols=symbols,
        kline_type=KLineType.DAILY,
        archive_path='archive'
    )
    backtest = Backtest(ts, strategy, feed)

    # 清理orders.csv, trades.csv, pnl.csv
    if os.path.exists('orders.csv'):
        os.remove('orders.csv')
    if os.path.exists('trades.csv'):
        os.remove('trades.csv')
    if os.path.exists('pnl.csv'):
        os.remove('pnl.csv')
    backtest.run(orders_path='orders.csv', trades_path='trades.csv', pnl_path='pnl.csv')