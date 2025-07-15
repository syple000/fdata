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
from fdata.market_data.market_data_fetcher import HistoricalData,StockInfo,FinancialData
from fdata.spider.spider_core import AntiDetectionSpider

class MarketDataDumper:
    def __init__(self, fetcher: MarketDataFetcher):
        self.fetcher = fetcher

    # 市场所有股票
    async def dump_stock_list(self, csv_dao: CSVGenericDAO[StockInfo]):
        await self.fetcher.fetch_stock_list(csv_dao)

    # 实时行情数据
    async def dump_realtime_data(self, symbols: List[str], csv_dao: CSVGenericDAO[RealTimeQuote], continue_signal: Callable[[], bool], send_event: Callable[[List[RealTimeQuote]], None]):
        while True:
            data = await self.fetcher.fetch_realtime_quotes(symbols, csv_dao)
            send_event(data)
            if not continue_signal():
                break

    # 历史行情数据
    async def dump_historical_data(self, symbols: List[str], start_date: str, end_date: str, csv_dao: CSVGenericDAO[RealTimeQuote], kline_type: KLineType, adjust_type: AdjustType):
        for symbol in symbols:
            await self.fetcher.fetch_historical_data(symbol, start_date, end_date, csv_dao)

    # 历史财务数据
    async def dump_financial_data(self, symbols: List[str], csv_dao: CSVGenericDAO[HistoricalData]):
        for symbol in symbols:
            await self.fetcher.fetch_financial_data(symbol, csv_dao)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('functions', help='Function to execute: stock_list,realtime,historical,financial')

    parser.add_argument('--directory', default='output', help='Directory to save CSV files')
    
    parser.add_argument('--symbols', default='', help='List of stock symbols (for realtime, historical, financial)')
    parser.add_argument('--batch_num', type=int, default=0, help='Batch num for processing symbols')
    
    parser.add_argument('--duration', type=int, default=30, help='Duration in seconds for realtime data fetching')
    
    parser.add_argument('--start_date', help='Start date for historical data (YYYY-MM-DD)')
    parser.add_argument('--end_date', help='End date for historical data (YYYY-MM-DD)')
    parser.add_argument('--kline_type', choices=['5m', '15m', '30m', '60m', 'daily', 'weekly', 'monthly'], default='daily', help='K-line type for historical data')
    parser.add_argument('--adjust_type', choices=['none', 'forward', 'backward'], default='none', help='Adjust type for historical data')

    parser.add_argument('--today_date', default=datetime.now().strftime('%Y-%m-%d'), help='Today date in YYYY-MM-DD format')

    args = parser.parse_args()
    if args.symbols:
        args.symbols = [symbol.strip() for symbol in args.symbols.split(',') if symbol.strip()]

    async def main():
        rate_limiter_mgr = RateLimiterManager()
        rate_limiter_mgr.add_rate_limiter('hq.sinajs.cn', RateLimiter(max_concurrent=1, min_interval=1, max_requests_per_minute=60)) # 秒级tick
        rate_limiter_mgr.add_rate_limiter('*.eastmoney.com', RateLimiter(max_concurrent=1, min_interval=5, max_requests_per_minute=20)) # 获取离线数据，5s间隔
        
        with ExitStack() as stack:
            async with AsyncExitStack() as async_stack:
                spider = await async_stack.enter_async_context(AntiDetectionSpider())
                fetcher = MarketDataFetcher(rate_limiter_mgr, spider)
                dumper = MarketDataDumper(fetcher)
 
                async def execute_function(function: str):
                    if function == 'stock_list':
                        csv_path = os.path.join(args.directory, 'stock_list', args.today_date)
                        if not os.path.exists(csv_path):
                            os.makedirs(csv_path)
                        if os.path.exists(os.path.join(csv_path, 'stock_list.csv')):
                            os.remove(os.path.join(csv_path, 'stock_list.csv'))
                        stock_info_csv_dao = stack.enter_context(CSVGenericDAO(os.path.join(csv_path, 'stock_list.csv'), StockInfo))
                        await dumper.dump_stock_list(stock_info_csv_dao)
                    elif function == 'realtime':
                        if not args.symbols:
                            raise ValueError("Symbols must be provided for realtime data")

                        csv_path = os.path.join(args.directory, 'realtime_quotes', args.today_date, str(args.batch_num))
                        if not os.path.exists(csv_path):
                            os.makedirs(csv_path)
                        if os.path.exists(os.path.join(csv_path, 'realtime_quotes.csv')):
                            os.remove(os.path.join(csv_path, 'realtime_quotes.csv'))
                        realtime_quote_csv_dao = stack.enter_context(CSVGenericDAO(os.path.join(csv_path, 'realtime_quotes.csv'), RealTimeQuote))

                        def create_timer_check_func(duration_seconds: int):
                            start_time = time.time()
                            def check_func():
                                return time.time() - start_time < duration_seconds
                            return check_func

                        continue_signal = create_timer_check_func(int(args.duration))
                        # 未来需要发送到交易平台 todo
                        send_event = lambda data: logging.info(f"Received {len(data)} realtime quotes")
                        await dumper.dump_realtime_data(args.symbols, realtime_quote_csv_dao, continue_signal, send_event)
                    elif function == 'historical':
                        if not args.symbols or not args.start_date or not args.end_date:
                            raise ValueError("Symbols, start_date, and end_date must be provided for historical data")

                        csv_path = os.path.join(args.directory, 'historical_data', args.today_date, str(args.batch_num), str(args.start_date)+'_'+str(args.end_date), str(args.kline_type), str(args.adjust_type))
                        if not os.path.exists(csv_path):
                            os.makedirs(csv_path)
                        if os.path.exists(os.path.join(csv_path, 'historical_data.csv')):
                            os.remove(os.path.join(csv_path, 'historical_data.csv'))

                        historical_data_csv_dao = stack.enter_context(CSVGenericDAO(os.path.join(csv_path, 'historical_data.csv'), HistoricalData))

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

                        await dumper.dump_historical_data(args.symbols, args.start_date, args.end_date, historical_data_csv_dao, kline_type, adjust_type)
                    elif function == 'financial':
                        if not args.symbols:
                            raise ValueError("Symbols must be provided for financial data")

                        csv_path = os.path.join(args.directory, 'financial_data', args.today_date, str(args.batch_num))
                        if not os.path.exists(csv_path):
                            os.makedirs(csv_path)
                        if os.path.exists(os.path.join(csv_path, 'financial_data.csv')):
                            os.remove(os.path.join(csv_path, 'financial_data.csv'))
                        financial_data_csv_dao = stack.enter_context(CSVGenericDAO(os.path.join(csv_path, 'financial_data.csv'), FinancialData))

                        await dumper.dump_financial_data(args.symbols, financial_data_csv_dao)
                    else:
                        raise ValueError(f"Invalid function: {function}")
                
                tasks = []
                functions = args.functions.split(',')
                for function in functions:
                    function = function.strip()
                    if function not in ['stock_list', 'realtime', 'historical', 'financial']:
                        raise ValueError(f"Invalid function: {function}")
                    tasks.append(asyncio.create_task(execute_function(function)))
                
                await asyncio.gather(*tasks)
               
    asyncio.run(main())