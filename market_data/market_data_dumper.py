from typing import List, Dict, Callable
import logging
from contextlib import ExitStack, AsyncExitStack
import argparse
import os
import time
import asyncio
from datetime import datetime
import pandas as pd

from fdata.dao.csv_dao import CSVGenericDAO
from fdata.spider.spider_core import AntiDetectionSpider
from fdata.spider.rate_limiter import RateLimiter, RateLimiterManager
from fdata.market_data.market_data_fetcher import MarketDataFetcher
from fdata.market_data.models import RealTimeQuote, KLineType, AdjustType, HistoricalData, Symbol, FinancialData, StockInfo, StockQuoteInfo, DividendInfo
from fdata.utils.rand_str import rand_str

class MarketDataDumper:
    def __init__(self, fetcher: MarketDataFetcher):
        self.fetcher = fetcher

    # 市场所有股票
    async def dump_stock_list(self, market_names: List[str], csv_dao: CSVGenericDAO[StockInfo]):
        for market_name in market_names:
            await self.fetcher.fetch_stock_list(market_name, csv_dao)
    
    # 获取股票公司类型信息
    async def dump_stock_company_type(self, csv_dao: CSVGenericDAO[StockInfo]):
        await self.fetcher.fetch_stock_company_type(csv_dao)

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
            if kline_type in [KLineType.MIN5, KLineType.MIN15, KLineType.MIN30, KLineType.MIN60] and adjust_type == AdjustType.NONE:
                await self.fetcher.fetch_historical_data(symbol, start_date, end_date, csv_dao, kline_type, adjust_type, from_='sina')
            else:
                await self.fetcher.fetch_historical_data(symbol, start_date, end_date, csv_dao, kline_type, adjust_type)

    # 历史财务数据
    async def dump_financial_data(self, symbols: List[Symbol], company_type_map: Dict[Symbol, str], csv_dao: CSVGenericDAO[HistoricalData]):
        for symbol in symbols:
            await self.fetcher.fetch_financial_data(symbol, company_type_map.get(symbol, ""), csv_dao)

    # 股票详情quote
    async def dump_stock_quote(self, symbols: List[Symbol], csv_dao: CSVGenericDAO[StockQuoteInfo]):
        for symbol in symbols:
            await self.fetcher.fetch_stock_quote(symbol, csv_dao)

    # 除权除息分红配股数据
    async def dump_dividend_info(self, symbols: List[Symbol], csv_dao: CSVGenericDAO[DividendInfo]):
        for symbol in symbols:
            await self.fetcher.fetch_dividend_info(symbol, csv_dao)


def chunk_symbols(symbols: List[Symbol], batch_size: int) -> List[List[Symbol]]:
    """将股票符号列表分割成指定大小的批次"""
    return [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

def create_timer_check_func(duration_seconds: int):
    start_time = time.time()
    def check_func():
        return time.time() - start_time < duration_seconds
    return check_func

def send_realtime_quotes(data):
    logging.info(f"Received realtime quotes: {data}")

def merge_data(path: str, df: pd.DataFrame, merge_on: str, sort_by: str) -> pd.DataFrame:
    if not os.path.exists(path):
        dfs = [df]
    else:
        existing_df = pd.read_csv(path, encoding='utf-8', dtype=str)
        dfs = [existing_df, df]

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=merge_on, keep='last')
    combined_df = combined_df.sort_values(by=sort_by)
    return combined_df

async def main(args):
    args.functions = [function.strip() for function in args.functions.split(',') if function.strip()]
    if args.archive_directory and not os.path.exists(args.archive_directory):
        os.makedirs(args.archive_directory)
    if args.market_names:
        args.market_names = [name.strip() for name in args.market_names.split(',') if name.strip()]
    if args.symbols_file: # 覆盖symbols参数
        df = pd.read_csv(args.symbols_file, encoding='utf-8', dtype=str).dropna(subset=['symbol'])
        args.symbols = ','.join(df['symbol'].tolist())
    if args.symbols:
        args.symbols = [Symbol.from_string(symbol.strip()) for symbol in args.symbols.split(',') if symbol.strip()]
    if args.duration:
        args.duration = int(args.duration)

    rate_limiter_mgr = RateLimiterManager()
    # 实时行情1s获取一次
    rate_limiter_mgr.add_rate_limiter('hq.sinajs.cn', RateLimiter(max_concurrent=1, min_interval=1, max_requests_per_minute=60)) # 秒级tick
    # 非实时数据5s获取一次
    rate_limiter_mgr.add_rate_limiter('quotes.sina.cn', RateLimiter(max_concurrent=1, min_interval=5, max_requests_per_minute=20))
    rate_limiter_mgr.add_rate_limiter('*.eastmoney.com', RateLimiter(max_concurrent=1, min_interval=5, max_requests_per_minute=20)) # 获取离线数据，5s间隔
    
    async with AsyncExitStack() as async_stack:
        spider = await async_stack.enter_async_context(AntiDetectionSpider())
        fetcher = MarketDataFetcher(rate_limiter_mgr, spider)
        dumper = MarketDataDumper(fetcher)

        async def execute_function(function: str):
            if function == 'stock_list':
                if not args.market_names:
                    raise ValueError("Market names must be provided for stock list data")

                company_type_df = None
                async def get_company_type():
                    nonlocal company_type_df
                    if company_type_df is not None:
                        return company_type_df
                    tmp_file_name = f"tmp_{rand_str(16)}.csv"
                    with CSVGenericDAO(tmp_file_name, StockInfo) as dao:
                        await dumper.dump_stock_company_type(dao)
                    company_type_df = pd.read_csv(tmp_file_name, encoding='utf-8', dtype=str)
                    os.remove(tmp_file_name)
                    return company_type_df

                for market_name in args.market_names:
                    dst_file_path = os.path.join(args.archive_directory, f'stock_list_{market_name}.csv')
                    if os.path.exists(dst_file_path) and args.write_mode == 'skip_existing':
                        logging.info(f"Skipping existing file: {dst_file_path}")
                        continue
                    tmp_file_name = f"tmp_{rand_str(16)}.csv"
                    with CSVGenericDAO(tmp_file_name, StockInfo) as dao:
                        await dumper.dump_stock_list([market_name], dao)
                    df = pd.read_csv(tmp_file_name, encoding='utf-8', dtype=str)
                    df.sort_values(by='symbol', inplace=True)
                    df = pd.merge(df[['symbol', 'name']], (await get_company_type())[['symbol', 'industry']], on='symbol', how='left')
                    df.to_csv(dst_file_path, index=False, encoding='utf-8')
                    os.remove(tmp_file_name)
            elif function == 'realtime':
                if not args.symbols:
                    raise ValueError("Symbols must be provided for realtime data")
                if not args.duration:
                    raise ValueError("Duration must be provided for realtime data")
                csv_paths = []
                tasks = []
                for symbols in chunk_symbols(args.symbols, 100):
                    tmp_file_name = f"tmp_{rand_str(16)}.csv"
                    continue_signal = create_timer_check_func(int(args.duration))
                    async def dump_realtime(tmp_file_name, symbols):
                        with CSVGenericDAO(tmp_file_name, RealTimeQuote) as dao:
                            await dumper.dump_realtime_data(symbols, dao, continue_signal, send_realtime_quotes)
                    tasks.append(asyncio.create_task(dump_realtime(tmp_file_name, symbols)))
                    csv_paths.append(tmp_file_name)
                await asyncio.gather(*tasks)

                dfs = []
                for csv_path in csv_paths:
                    dfs.append(pd.read_csv(csv_path, encoding='utf-8', dtype=str))
                    os.remove(csv_path)
                df = pd.concat(dfs, ignore_index=True).sort_values(by='timestamp')

                for symbol, grouped_df in df.groupby('symbol'):
                    symbol_dir = os.path.join(args.archive_directory, symbol)
                    if not os.path.exists(symbol_dir):
                        os.makedirs(symbol_dir)
                    csv_path = os.path.join(symbol_dir, f'realtime_quotes_{datetime.now().strftime("%Y-%m-%d")}.csv')
                    merge_data(csv_path, grouped_df, 'timestamp', 'timestamp').to_csv(csv_path, index=False, encoding='utf-8')
            elif function == 'historical':
                if not args.symbols:
                    raise ValueError("Symbols must be provided for historical data")
                if not args.start_date or not args.end_date:
                    raise ValueError("Start date and end date must be provided for historical data")
                if not args.kline_types:
                    raise ValueError("K-line type must be provided for historical data")
                if not args.adjust_type:
                    raise ValueError("Adjust type must be provided for historical data")

                kline_types = []
                for kline_type in args.kline_types.split(','):
                    kline_type = kline_type.strip()
                    if kline_type == '5m':
                        kline_types.append(KLineType.MIN5)
                    elif kline_type == '15m':
                        kline_types.append(KLineType.MIN15)
                    elif kline_type == '30m':
                        kline_types.append(KLineType.MIN30)
                    elif kline_type == '60m':
                        kline_types.append(KLineType.MIN60)
                    elif kline_type == 'daily':
                        kline_types.append(KLineType.DAILY)
                    elif kline_type == 'weekly':
                        kline_types.append(KLineType.WEEKLY)
                    elif kline_type == 'monthly':
                        kline_types.append(KLineType.MONTHLY)
                    else:
                        raise ValueError(f"Invalid kline_type: {kline_type}")

                if args.adjust_type == 'none':
                    adjust_type = AdjustType.NONE
                elif args.adjust_type == 'forward':
                    adjust_type = AdjustType.FORWARD
                elif args.adjust_type == 'backward':
                    adjust_type = AdjustType.BACKWARD
                else:
                    raise ValueError(f"Invalid adjust_type: {args.adjust_type}")

                tasks = []
                for kline_type in kline_types:
                    async def dump_historical_data(kline_type):
                        for symbol in args.symbols:
                            dst_file_path = os.path.join(args.archive_directory, symbol.to_string(), f'historical_data_{kline_type.name}_{adjust_type.name}.csv')
                            if os.path.exists(dst_file_path) and args.write_mode == 'skip_existing':
                                logging.info(f"Skipping existing file: {dst_file_path}")
                                continue
                            tmp_file_name = f"tmp_{rand_str(16)}.csv"
                            with CSVGenericDAO(tmp_file_name, HistoricalData) as dao:
                                await dumper.dump_historical_data([symbol], args.start_date, args.end_date, dao, kline_type, adjust_type)
                            df = pd.read_csv(tmp_file_name, encoding='utf-8', dtype=str)
                            if not os.path.exists(os.path.dirname(dst_file_path)):
                                os.makedirs(os.path.dirname(dst_file_path))
                            merge_data(dst_file_path, df, 'date', 'date').to_csv(dst_file_path, index=False, encoding='utf-8')
                            os.remove(tmp_file_name)
                    tasks.append(asyncio.create_task(dump_historical_data(kline_type)))
                await asyncio.gather(*tasks)
            elif function == 'financial':
                if not args.symbols:
                    raise ValueError("Symbols must be provided for financial data")

                company_type_map = None
                async def get_company_type():
                    nonlocal company_type_map
                    if company_type_map is not None:
                        return company_type_map
                    tmp_file_name = f"tmp_{rand_str(16)}.csv"
                    with CSVGenericDAO(tmp_file_name, StockInfo) as dao:
                        await dumper.dump_stock_company_type(dao)
                    company_type_df = pd.read_csv(tmp_file_name, encoding='utf-8', dtype=str)
                    company_type_map = {}
                    for index, row in company_type_df.iterrows():
                        company_type_map[Symbol.from_string(row['symbol'])] = row['industry']
                    os.remove(tmp_file_name)
                    return company_type_map

                for symbol in args.symbols:
                    dst_file_path = os.path.join(args.archive_directory, symbol.to_string(), 'financial_data.csv')
                    if os.path.exists(dst_file_path) and args.write_mode == 'skip_existing':
                        logging.info(f"Skipping existing file: {dst_file_path}")
                        continue
                    company_type_map = await get_company_type()  # 公司类型数据加载
                    tmp_file_name = f"tmp_{rand_str(16)}.csv"
                    with CSVGenericDAO(tmp_file_name, FinancialData) as dao:
                        await dumper.dump_financial_data([symbol], company_type_map, dao)
                    df = pd.read_csv(tmp_file_name, encoding='utf-8', dtype=str)
                    if not os.path.exists(os.path.dirname(dst_file_path)):
                        os.makedirs(os.path.dirname(dst_file_path))
                    merge_data(dst_file_path, df, 'report_date', 'report_date').to_csv(dst_file_path, index=False, encoding='utf-8')
                    os.remove(tmp_file_name)
            elif function == 'stock_quote':
                if not args.symbols:
                    raise ValueError("Symbols must be provided for stock quote data")
                for symbol in args.symbols:
                    dst_file_path = os.path.join(args.archive_directory, symbol.to_string(), f'stock_quote_{datetime.now().strftime("%Y-%m-%d")}.csv')
                    if os.path.exists(dst_file_path) and args.write_mode == 'skip_existing':
                        logging.info(f"Skipping existing file: {dst_file_path}")
                        continue
                    tmp_file_name = f"tmp_{rand_str(16)}.csv"
                    with CSVGenericDAO(tmp_file_name, StockQuoteInfo) as dao:
                        await dumper.dump_stock_quote([symbol], dao)
                    df = pd.read_csv(tmp_file_name, encoding='utf-8', dtype=str)
                    if not os.path.exists(os.path.dirname(dst_file_path)):
                        os.makedirs(os.path.dirname(dst_file_path))
                    df.to_csv(dst_file_path, index=False, encoding='utf-8')
                    os.remove(tmp_file_name)
            elif function == 'dividend_info':
                if not args.symbols:
                    raise ValueError("Symbols must be provided for dividend info data")
                for symbol in args.symbols:
                    dst_file_path = os.path.join(args.archive_directory, symbol.to_string(), 'dividend_info.csv')
                    if os.path.exists(dst_file_path) and args.write_mode == 'skip_existing':
                        logging.info(f"Skipping existing file: {dst_file_path}")
                        continue
                    tmp_file_name = f"tmp_{rand_str(16)}.csv"
                    with CSVGenericDAO(tmp_file_name, DividendInfo) as dao:
                        await dumper.dump_dividend_info([symbol], dao)
                    df = pd.read_csv(tmp_file_name, encoding='utf-8', dtype=str)
                    if not os.path.exists(os.path.dirname(dst_file_path)):
                        os.makedirs(os.path.dirname(dst_file_path))
                    merge_data(dst_file_path, df, 'plan_notice_date', 'plan_notice_date').to_csv(dst_file_path, index=False, encoding='utf-8')
                    os.remove(tmp_file_name)
            else:
                raise ValueError(f"Invalid function: {function}")
        
        tasks = []
        for function in args.functions:
            tasks.append(asyncio.create_task(execute_function(function)))
        await asyncio.gather(*tasks)
        
if __name__ == "__main__":
    today = datetime.now().strftime('%Y-%m-%d')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser(description="Market Data Dumper")
    parser.add_argument('--functions', type=str, required=True, help="Comma-separated list of functions to execute (e.g., stock_list,realtime,historical,financial,stock_quote,dividend_info)")
    parser.add_argument('--archive_directory', type=str, default='archive', help="Directory to store archived data")
    parser.add_argument('--write_mode', type=str, default='skip_existing', choices=['skip_existing', 'default'], help="Write mode for CSV files. skip_existing will skip existing file, default will merge existing file and fetched data(for historical, financial, dividend) or overwrite(for stock_list, stock_quote).")
    parser.add_argument('--market_names', type=str, default='上证指数,深证成指,北交所,沪深300', help="Comma-separated list of market names (e.g., SH,SZ,BJ)")
    parser.add_argument('--symbols_file', type=str, default='', help="File containing stock symbols, one symbol per line") # 如果有symbols_file，则symbols参数无效
    parser.add_argument('--symbols', type=str, default='', help="Comma-separated list of stock symbols (e.g., 600000.SH , 000001.SZ)")
    parser.add_argument('--duration', type=int, default=int(datetime.strptime(today + ' 16:00:00', '%Y-%m-%d %H:%M:%S').timestamp() - datetime.now().timestamp()), help="Duration in seconds for realtime data")
    parser.add_argument('--start_date', type=str, default='2001-01-01', help="Start date for historical data (YYYY-MM-DD)")
    parser.add_argument('--end_date', type=str, default=today, help="End date for historical data (YYYY-MM-DD)")
    parser.add_argument('--kline_types', type=str, default='daily', help="K-line type for historical data")
    parser.add_argument('--adjust_type', type=str, default='forward', choices=['none', 'forward', 'backward'], help="Adjust type for historical data")

    args = parser.parse_args()

    asyncio.run(main(args))
