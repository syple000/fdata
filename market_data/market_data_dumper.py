from typing import List, Callable
import logging
from contextlib import ExitStack, AsyncExitStack
import argparse
import os
import time
import asyncio
from datetime import datetime

from fdata.dao.csv_dao import CSVGenericDAO
from fdata.market_data.market_data_fetcher import MarketDataFetcher, RealTimeQuote, RateLimiterManager, RateLimiter
from fdata.market_data.market_data_fetcher import KLineType, AdjustType
from fdata.market_data.market_data_fetcher import HistoricalData, Symbol, FinancialData, StockInfo
from fdata.spider.spider_core import AntiDetectionSpider

class MarketDataDumper:
    def __init__(self, fetcher: MarketDataFetcher):
        self.fetcher = fetcher

    # 市场所有股票
    async def dump_stock_list(self, market_names: List[str], csv_dao: CSVGenericDAO[StockInfo]):
        await self.fetcher.fetch_stock_list(market_names, csv_dao)

    # 实时行情数据
    async def dump_realtime_data(self, symbols: List[Symbol], csv_dao: CSVGenericDAO[RealTimeQuote], continue_signal: Callable[[], bool], send_event: Callable[[List[RealTimeQuote]], None]):
        while True:
            data = await self.fetcher.fetch_realtime_quotes(symbols, csv_dao)
            send_event(data)
            if not continue_signal():
                break

    # 历史行情数据
    async def dump_historical_data(self, symbols: List[Symbol], start_date: str, end_date: str, csv_dao: CSVGenericDAO[RealTimeQuote], kline_type: KLineType, adjust_type: AdjustType):
        for symbol in symbols:
            await self.fetcher.fetch_historical_data(symbol, start_date, end_date, csv_dao, kline_type, adjust_type)

    # 历史财务数据
    async def dump_financial_data(self, symbols: List[Symbol], csv_dao: CSVGenericDAO[HistoricalData]):
        for symbol in symbols:
            await self.fetcher.fetch_financial_data(symbol, csv_dao)

def chunk_symbols(symbols: List[Symbol], batch_size: int) -> List[List[Symbol]]:
    """将股票符号列表分割成指定大小的批次"""
    return [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('functions', help='Function to execute: stock_list,realtime,historical,financial')

    parser.add_argument('--directory', default='output', help='Directory to save CSV files')
    
    parser.add_argument('--symbols', default='', help='List of stock symbols (for realtime, historical, financial)')
    
    parser.add_argument('--duration', type=int, default=30, help='Duration in seconds for realtime data fetching')
    
    parser.add_argument('--start_date', help='Start date for historical data (YYYY-MM-DD)')
    parser.add_argument('--end_date', help='End date for historical data (YYYY-MM-DD)')
    parser.add_argument('--kline_type', choices=['5m', '15m', '30m', '60m', 'daily', 'weekly', 'monthly'], default='daily', help='K-line type for historical data')
    parser.add_argument('--adjust_type', choices=['none', 'forward', 'backward'], default='none', help='Adjust type for historical data')
    
    parser.add_argument('--market_names', default='上证指数,深证成指,北交所', help='List of market names for stock list fetching')

    parser.add_argument('--today_date', default=datetime.now().strftime('%Y-%m-%d'), help='Today date in YYYY-MM-DD format')

    args = parser.parse_args()
    if args.symbols:
        args.symbols = [Symbol.from_string(symbol.strip()) for symbol in args.symbols.split(',') if symbol.strip()]
    if args.market_names:
        args.market_names = [name.strip() for name in args.market_names.split(',') if name.strip()]

    async def main():
        rate_limiter_mgr = RateLimiterManager()
        rate_limiter_mgr.add_rate_limiter('hq.sinajs.cn', RateLimiter(max_concurrent=1, min_interval=1, max_requests_per_minute=60)) # 秒级tick
        rate_limiter_mgr.add_rate_limiter('*.eastmoney.com', RateLimiter(max_concurrent=1, min_interval=5, max_requests_per_minute=20)) # 获取离线数据，5s间隔
        
        async with AsyncExitStack() as async_stack:
            spider = await async_stack.enter_async_context(AntiDetectionSpider())
            fetcher = MarketDataFetcher(rate_limiter_mgr, spider)
            dumper = MarketDataDumper(fetcher)

            # symbols过多时，需要做切割，分多批次执行
            symbol_chunks = chunk_symbols(args.symbols, 100) if args.symbols else []

            async def execute_function(function: str, symbols: List[str], batch_num: int):
                if function == 'stock_list':
                    csv_path = os.path.join(args.directory, 'stock_list', args.today_date)
                    if not os.path.exists(csv_path):
                        os.makedirs(csv_path)
                    if os.path.exists(os.path.join(csv_path, f"{'.'.join(args.market_names)}_stock_list.csv")):
                        os.remove(os.path.join(csv_path, f"{'.'.join(args.market_names)}_stock_list.csv"))
                    with CSVGenericDAO(os.path.join(csv_path, f"{'.'.join(args.market_names)}_stock_list.csv"), StockInfo) as stock_info_csv_dao:
                        await dumper.dump_stock_list(args.market_names, stock_info_csv_dao)
                elif function == 'realtime':
                    if not symbols:
                        raise ValueError("Symbols must be provided for realtime data")

                    csv_path = os.path.join(args.directory, 'realtime_quotes', args.today_date, str(batch_num))
                    if not os.path.exists(csv_path):
                        os.makedirs(csv_path)
                    if os.path.exists(os.path.join(csv_path, 'realtime_quotes.csv')):
                        os.remove(os.path.join(csv_path, 'realtime_quotes.csv'))

                    def create_timer_check_func(duration_seconds: int):
                        start_time = time.time()
                        def check_func():
                            return time.time() - start_time < duration_seconds
                        return check_func

                    continue_signal = create_timer_check_func(int(args.duration))
                    # 未来需要发送到交易平台 todo
                    send_event = lambda data: logging.info(f"Received realtime quotes")
                    with CSVGenericDAO(os.path.join(csv_path, 'realtime_quotes.csv'), RealTimeQuote) as realtime_quote_csv_dao:
                        await dumper.dump_realtime_data(symbols, realtime_quote_csv_dao, continue_signal, send_event)
                elif function == 'historical':
                    if not symbols or not args.start_date or not args.end_date:
                        raise ValueError("Symbols, start_date, and end_date must be provided for historical data")

                    csv_path = os.path.join(args.directory, 'historical_data', args.today_date, str(batch_num), str(args.start_date)+'_'+str(args.end_date), str(args.kline_type), str(args.adjust_type))
                    if not os.path.exists(csv_path):
                        os.makedirs(csv_path)
                    if os.path.exists(os.path.join(csv_path, 'historical_data.csv')):
                        os.remove(os.path.join(csv_path, 'historical_data.csv'))

                    if args.kline_type == '5m':
                        kline_type = KLineType.MIN5
                    elif args.kline_type == '15m':
                        kline_type = KLineType.MIN15
                    elif args.kline_type == '30m':
                        kline_type = KLineType.MIN30
                    elif args.kline_type == '60m':
                        kline_type = KLineType.MIN60
                    elif args.kline_type == 'daily':
                        kline_type = KLineType.DAILY
                    elif args.kline_type == 'weekly':
                        kline_type = KLineType.WEEKLY
                    elif args.kline_type == 'monthly':
                        kline_type = KLineType.MONTHLY
                    else:
                        raise ValueError(f"Invalid kline_type: {args.kline_type}")

                    if args.adjust_type == 'none':
                        adjust_type = AdjustType.NONE
                    elif args.adjust_type == 'forward':
                        adjust_type = AdjustType.FORWARD
                    elif args.adjust_type == 'backward':
                        adjust_type = AdjustType.BACKWARD
                    else:
                        raise ValueError(f"Invalid adjust_type: {args.adjust_type}")

                    with CSVGenericDAO(os.path.join(csv_path, 'historical_data.csv'), HistoricalData) as historical_data_csv_dao:
                        await dumper.dump_historical_data(symbols, args.start_date, args.end_date, historical_data_csv_dao, kline_type, adjust_type)
                elif function == 'financial':
                    if not symbols:
                        raise ValueError("Symbols must be provided for financial data")

                    csv_path = os.path.join(args.directory, 'financial_data', args.today_date, str(batch_num))
                    if not os.path.exists(csv_path):
                        os.makedirs(csv_path)
                    if os.path.exists(os.path.join(csv_path, 'financial_data.csv')):
                        os.remove(os.path.join(csv_path, 'financial_data.csv'))
                    with CSVGenericDAO(os.path.join(csv_path, 'financial_data.csv'), FinancialData) as financial_data_csv_dao:
                        await dumper.dump_financial_data(symbols, financial_data_csv_dao)
                else:
                    raise ValueError(f"Invalid function: {function}")
            
            tasks = []
            functions = args.functions.split(',')
            for function in functions:
                function = function.strip()
                if function not in ['stock_list', 'realtime', 'historical', 'financial']:
                    raise ValueError(f"Invalid function: {function}")
                for index, symbols in enumerate(symbol_chunks):
                    tasks.append(asyncio.create_task(execute_function(function, symbols, index)))
            
            await asyncio.gather(*tasks)
               
    asyncio.run(main())