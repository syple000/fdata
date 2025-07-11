import asyncio
import json
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from urllib.parse import urlencode
import requests
import logging
import random
from ..spider.rate_limiter import RateLimiter, RateLimiterManager
from ..spider.spider_core import AntiDetectionSpider
from ..utils.retry import retry
from ..utils.bytes_str_convert import from_bytes_to_str, from_str_to_bytes
from ..utils.call_loop import async_call_loop

@dataclass
class RealTimeQuote:
    """实时行情数据结构"""
    symbol: str  # 股票代码
    name: str    # 股票名称
    price: float # 当前价格
    change: float # 涨跌额
    change_percent: float # 涨跌幅
    volume: int  # 成交量
    turnover: float # 成交额
    open_price: float # 开盘价
    high_price: float # 最高价
    low_price: float  # 最低价
    prev_close: float # 昨收价
    timestamp: str # 数据时间 2023-10-01 09:30:00


@dataclass
class HistoricalData:
    """历史行情数据结构"""
    symbol: str
    date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    turnover: float
    change_percent: float


@dataclass
class StockInfo:
    """股票基本信息"""
    symbol: str
    name: str
    market: str  # 市场类型：SH/SZ
    industry: str
    list_date: str


@dataclass
class FinancialData:
    """财务数据结构"""
    symbol: str
    report_date: str
    revenue: float  # 营业收入
    net_profit: float  # 净利润
    eps: float  # 每股收益
    roe: float  # 净资产收益率
    total_assets: float  # 总资产
    total_liabilities: float  # 总负债


@dataclass
class DividendData:
    """除权除息数据结构"""
    symbol: str
    ex_date: str  # 除权除息日
    dividend_per_share: float  # 每股分红
    bonus_share_ratio: float   # 送股比例
    rights_issue_ratio: float  # 配股比例
    rights_issue_price: float  # 配股价格


class MarketDataFetcher:
    """市场数据获取器"""
    
    def __init__(self, rate_limiter_mgr: RateLimiterManager, spider: AntiDetectionSpider):
        self.rate_limiter_mgr = rate_limiter_mgr
        self.spider = spider
        
        # 新浪财经实时行情headers
        self.sina_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://finance.sina.com.cn/",
        }
        
        # 东方财富headers
        self.eastmoney_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36",
            "Referer": "http://quote.eastmoney.com/",
        }

    @retry(max_retries=3, delay=1)
    async def fetch_realtime_quotes(self, symbols: List[str]) -> List[RealTimeQuote]:
        """
        从新浪财经获取实时行情
        
        Args:
            symbols: 股票代码列表，格式如['000001', '000002']
        
        Returns:
            实时行情数据列表
        """
        
        # 转换股票代码格式：000001 -> sz000001, 600000 -> sh600000
        sina_symbols = []
        for symbol in symbols:
            if symbol.startswith('6'):
                sina_symbols.append(f'sh{symbol}')
            else:
                sina_symbols.append(f'sz{symbol}')
        
        # 新浪实时行情API：返回JavaScript格式数据
        # 参数：list为股票代码，用逗号分隔
        url = f"https://hq.sinajs.cn/list={','.join(sina_symbols)}"

        async with self.rate_limiter_mgr.get_rate_limiter('hq.sinajs.cn'):
            response = await self.spider.crawl_url(url, headers=self.sina_headers, filter_func=lambda x: x.url == url)
        
        if not response or not response.success or not response.data_processor.responses:
            raise Exception(f"Failed to fetch realtime quotes: {response.status if response else 'No response'}")
        
        quotes = []
        lines = from_str_to_bytes(response.data_processor.responses[0].body).decode('utf-8').split('\n')

        for i, line in enumerate(lines):
            if i >= len(symbols):
                break

            if '=' not in line:
                raise Exception(f"Invalid data format in line {i + 1}: {line}")
                
            # 解析新浪返回的数据格式
            # var hq_str_sz000001="平安银行,10.13,10.14,10.11,10.17,10.08,10.11,10.12,..."
            left_part, right_part, = line.split('=')
            if symbols[i] not in left_part:
                raise Exception(f"Symbol mismatch: {symbols[i]} not found in {left_part}")

            fields = right_part.strip('";\n').split(',')
            if len(fields) < 32:
                raise Exception(f"Insufficient data fields for symbol {symbols[i]}: {fields}")
            
            quote = RealTimeQuote(
                symbol=symbols[i],
                name=fields[0],                    # 股票名称
                price=float(fields[3]),            # 当前价格
                change=float(fields[3]) - float(fields[2]),  # 涨跌额
                change_percent=((float(fields[3]) - float(fields[2])) / float(fields[2])) * 100,  # 涨跌幅
                volume=int(fields[8]),             # 成交量(股)
                turnover=float(fields[9]),         # 成交额
                open_price=float(fields[1]),       # 开盘价
                high_price=float(fields[4]),       # 最高价
                low_price=float(fields[5]),        # 最低价
                prev_close=float(fields[2]),       # 昨收价
                timestamp=f"{fields[30]} {fields[31]}"  # 行情时间
            )
            quotes.append(quote)
        
        logging.info(f"Fetched {len(quotes)} realtime quotes for symbols: {', '.join(symbols)}, detail info: {', '.join([f'{q.symbol}: {q.price} ({q.change_percent:.2f}%)' for q in quotes])}")
        return quotes

    @retry(max_retries=3, delay=1)
    async def fetch_historical_data(self, symbol: str, start_date: str, end_date: str, klt: str='101', fqt: str='0') -> List[HistoricalData]:
        """
        从东方财富获取历史行情数据
        
        Args:
            symbol: 股票代码，如'000001'
            start_date: 开始日期，格式'YYYY-MM-DD'
            end_date: 结束日期，格式'YYYY-MM-DD'
        
        Returns:
            历史行情数据列表
        """

        # 转换股票代码格式
        if symbol.startswith('6'):
            secid = f'1.{symbol}'  # 沪市
        else:
            secid = f'0.{symbol}'  # 深市
        
        # 东方财富历史数据API
        # 参数说明：
        # secid: 证券ID，格式为市场代码.股票代码
        # klt: K线类型，101=日K线，102=周K线，103=月K线，5=5分钟，15=15分钟，30=30分钟，60=60分钟
        # fqt: 复权类型，1=前复权，2=后复权，0=不复权
        # beg: 开始日期
        # end: 结束日期
        params = {
            'secid': secid,
            'klt': klt,  # 日K线
            'fqt': fqt,    # 前复权
            'beg': start_date.replace('-', ''),
            'end': end_date.replace('-', ''),
            'fields1': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        }
        
        url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?{urlencode(params)}"
        logging.info(f"Fetching historical data for {symbol} from {start_date} to {end_date}, URL: {url}")
        
        async with self.rate_limiter_mgr.get_rate_limiter('push2his.eastmoney.com'):
            response = await self.spider.crawl_url(url, headers=self.eastmoney_headers, filter_func=lambda x: x.url == url)
        
        if not response or not response.success or not response.data_processor.responses:
            raise Exception(f"Failed to fetch historical data for {symbol}: {response.status if response else 'No response'}")
        
        data = json.loads(from_str_to_bytes(response.data_processor.responses[0].body).decode('utf-8'))
        
        if data['rc'] != 0 or not data.get('data', {}).get('klines'):
            raise Exception(f"No historical data found for {symbol}")
        
        historical_data = []
        for kline in data['data']['klines']:
            # 数据格式：日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
            fields = kline.split(',')
            
            historical_data.append(HistoricalData(
                symbol=symbol,
                date=fields[0],
                open_price=float(fields[1]),
                high_price=float(fields[3]),
                low_price=float(fields[4]),
                close_price=float(fields[2]),
                volume=int(fields[5]),
                turnover=float(fields[6]),
                change_percent=float(fields[8])
            ))
        
        logging.info(f"Fetched {len(historical_data)} historical data records for {symbol} from {start_date} to {end_date}, klines: {', '.join([f'{hd.date}: {hd.close_price} ({hd.change_percent:.2f}%)' for hd in historical_data])}")
        return historical_data

    @retry(max_retries=3, delay=1)
    async def fetch_stock_list(self, market: str = 'all') -> List[StockInfo]:
        """
        从东方财富获取股票列表
        
        Args:
            market: 市场类型，'sh'=沪市，'sz'=深市，'all'=全部
        
        Returns:
            股票信息列表
        """
        # 东方财富股票列表API
        # 参数说明：
        # pz: 每页数量（最多100条）
        # pn: 页码
        # po: 排序方式
        # fields: 返回字段
        
        all_stocks = []
        page = 1
        
        while True:
            params = {
                'pz': '100',  # 每页100条（最大值）
                'pn': str(page),
                'po': '1',     # 按代码排序
                'fields': 'f1,f2,f3,f4,f12,f13,f14',  # 返回字段：涨跌幅,最新价,涨跌额,成交量,代码,名称,行业
            }
            
            if market == 'sh':
                params['fs'] = 'm:1'  # 沪市
            elif market == 'sz':
                params['fs'] = 'm:0'  # 深市
            else:
                params['fs'] = 'm:0+m:1'  # 全部
            
            url = f"https://push2.eastmoney.com/api/qt/clist/get?{urlencode(params)}"
            
            async with self.rate_limiter_mgr.get_rate_limiter('push2.eastmoney.com'):
                response = await self.spider.crawl_url(url, headers=self.eastmoney_headers, filter_func=lambda x: x.url == url)
            
            if not response or not response.success or not response.data_processor.responses:
                raise Exception(f"Failed to fetch stock list: {response.status if response else 'No response'}")
            
            data = json.loads(from_str_to_bytes(response.data_processor.responses[0].body).decode('utf-8'))
            
            if data['rc'] != 0 or not data.get('data', {}).get('diff'):
                break
            
            page_stocks = []
            for _, item in data['data']['diff'].items():
                symbol = item['f12']
            
                # 过滤掉转债：转债代码通常以12或11开头，且为6位数字
                if (symbol.startswith('12') or symbol.startswith('11')) and len(symbol) == 6:
                    continue
                
                # 过滤掉其他非股票代码（如指数、基金等）
                if not (symbol.startswith('0') or symbol.startswith('3') or symbol.startswith('6')):
                    continue
                
                market_code = 'SH' if symbol.startswith('6') else 'SZ'

                page_stocks.append(StockInfo(
                    symbol=symbol,
                    name=item['f14'],
                    market=market_code,
                    industry=item.get('f13', ''),
                    list_date=''  # 此API不返回上市日期
                ))

            all_stocks.extend(page_stocks)
            
            # 如果当前页数据少于100条，说明已经是最后一页
            if len(data['data']['diff']) < 100:
                break
            
            page += 1
        
        logging.info(f"Fetched {len(all_stocks)} stocks for market: {market}")
        return all_stocks

    @retry(max_retries=3, delay=1)
    async def fetch_financial_data(self, symbol: str, report_type: str = 'annual') -> List[FinancialData]:
        """
        从东方财富获取财务数据，参考 akshare 实现
        
        Args:
            symbol: 股票代码
            report_type: 报告类型，'annual'=年报，'quarterly'=季报
        
        Returns:
            财务数据列表
        """
        # 东方财富财务数据API
        # 参数说明：
        # reportName: 数据表名，RPT_LICO_FN_CPD=利润表
        # filter: 过滤条件，指定股票代码
        # sortColumns: 排序字段
        # sortTypes: 排序方式，-1=降序，1=升序
        # pageSize: 每页数量
        # pageNumber: 页码
        # columns: 返回字段
        financial_data = []
        page_number = 1
        page_size = 50  # 每页数量

        while True:
            params = {
                'reportName': 'RPT_LICO_FN_CPD',  # 利润表
                'filter': f'(SECURITY_CODE="{symbol}")',
                'sortColumns': 'REPORTDATE',
                'sortTypes': '-1',
                'pageSize': str(page_size),
                'pageNumber': str(page_number),
                'columns': 'ALL',
            }
            
            url = f"https://datacenter-web.eastmoney.com/api/data/v1/get?{urlencode(params)}"
            async with self.rate_limiter_mgr.get_rate_limiter('datacenter-web.eastmoney.com'):
                response = await self.spider.crawl_url(url, headers=self.eastmoney_headers, filter_func=lambda x: x.url == url)
            
            if not response or not response.success or not response.data_processor.responses:
                raise Exception(f"Failed to fetch financial data for {symbol}: {response.status if response else 'No response'}")
            
            data = json.loads(from_str_to_bytes(response.data_processor.responses[0].body).decode('utf-8'))
            if not data.get('result') or not data['result'].get('data'):
                break  # 没有更多数据
            
            page_data = data['result']['data']
            for item in page_data:
                financial_data.append(FinancialData(
                    symbol=symbol,
                    report_date=item.get('REPORTDATE', ''),
                    revenue=float(item.get('TOTAL_OPERATE_INCOME', 0)) if 'TOTAL_OPERATE_INCOME' in item and item['TOTAL_OPERATE_INCOME'] else 0,
                    net_profit=float(item.get('PARENT_NETPROFIT', 0)) if 'PARENT_NETPROFIT' in item and item['PARENT_NETPROFIT'] else 0,
                    eps=float(item.get('BASIC_EPS', 0)) if 'BASIC_EPS' in item and item['BASIC_EPS'] else 0,
                    roe=float(item.get('WEIGHTAVG_ROE', 0)) if 'WEIGHTAVG_ROE' in item and item['WEIGHTAVG_ROE'] else 0,
                    total_assets=float(item.get('TOTAL_ASSETS', 0)) if 'TOTAL_ASSETS' in item and item['TOTAL_ASSETS'] else 0,
                    total_liabilities=float(item.get('TOTAL_LIABILITIES', 0)) if 'TOTAL_LIABILITIES' in item and item['TOTAL_LIABILITIES'] else 0,
                ))
            
            # 如果当前页数据少于page_size，说明已经是最后一页
            if len(page_data) < page_size:
                break
            
            page_number += 1  # 查询下一页

        logging.info(f"Fetched a total of {len(financial_data)} financial data records for {symbol}")
        return financial_data

    @retry(max_retries=3, delay=1)
    async def fetch_dividend_data(self, symbol: str) -> List[DividendData]:
        """
        从东方财富获取除权除息数据
        
        Args:
            symbol: 股票代码
        
        Returns:
            除权除息数据列表
        """
        
        # 东方财富除权除息数据API
        dividend_data = []
        page_number = 1
        page_size = 50  # 每页数量

        while True:
            params = {
                'reportName': 'RPT_SHAREBONUS_DET',
                'columns': 'ALL',  # 返回所有字段
                'quoteColumns': '',
                'pageNumber': str(page_number),  # 页码
                'pageSize': str(page_size),     # 每页数量
                'sortColumns': 'PLAN_NOTICE_DATE',  # 排序字段
                'sortTypes': '1',   # 升序排序
                'source': 'WEB',
                'client': 'WEB',
                'filter': f'(SECURITY_CODE="{symbol}")'  # 过滤条件，指定股票代码
            }
            
            url = f"https://datacenter-web.eastmoney.com/api/data/v1/get?{urlencode(params)}"

            logging.info(f"Fetching dividend data for {symbol}, page {page_number}, URL: {url}")
            
            async with self.rate_limiter_mgr.get_rate_limiter('datacenter-web.eastmoney.com'):
                response = await self.spider.crawl_url(url, headers=self.eastmoney_headers, filter_func=lambda x: x.url == url)
            
            if not response or not response.success or not response.data_processor.responses:
                raise Exception(f"Failed to fetch dividend data for {symbol}: {response.status if response else 'No response'}")
            
            data = json.loads(from_str_to_bytes(response.data_processor.responses[0].body).decode('utf-8'))
            
            if not data.get('result') or not data['result'].get('data'):
                break  # 没有更多数据
            
            for item in data['result']['data']:
                dividend_data.append(DividendData(
                    symbol=symbol,
                    ex_date=item.get('EX_DIVIDEND_DATE', ''),
                    dividend_per_share=float(item.get('CASH_DIVIDEND_RATIO', 0)) / 10,  # 转换为每股分红
                    bonus_share_ratio=float(item.get('BONUS_SHARE_RATIO', 0)) / 10,     # 转换为每10股送股数
                    rights_issue_ratio=float(item.get('ALLOTMENT_RATIO', 0)) / 10,      # 转换为每10股配股数
                    rights_issue_price=float(item.get('ALLOTMENT_PRICE', 0))
                ))
            
            # 如果当前页数据少于page_size，说明已经是最后一页
            if len(data['result']['data']) < page_size:
                break
            
            page_number += 1  # 查询下一页
        
        logging.info(f"Fetched {len(dividend_data)} dividend data records for {symbol}")
        return dividend_data

    def to_dict(self, data_objects: List[Any]) -> List[Dict]:
        """将数据对象转换为字典格式，便于持久化存储"""
        return [asdict(obj) for obj in data_objects]

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # 示例用法
    async def main():
        rate_limiter_mgr = RateLimiterManager()
        rate_limiter_mgr.add_rate_limiter('hq.sinajs.cn', RateLimiter(max_concurrent=5, max_requests_per_minute=60))
        rate_limiter_mgr.add_rate_limiter('*.eastmoney.com', RateLimiter(max_concurrent=1, min_interval=5, max_requests_per_minute=15))
        
        async with AntiDetectionSpider() as spider:
            fetcher = MarketDataFetcher(rate_limiter_mgr, spider)

            # 创建定时器函数，运行指定时间后返回False
            def create_timer_check_func(duration_seconds: int):
                start_time = time.time()
                def check_func():
                    return time.time() - start_time < duration_seconds
                return check_func
            
            tasks = [
                async_call_loop(fetcher.fetch_realtime_quotes, ['000001'], interval=1.0, check_func=create_timer_check_func(30), ignore_exceptions=True),  # 运行30秒
                async_call_loop(fetcher.fetch_realtime_quotes, ['000002'], interval=1.0, check_func=create_timer_check_func(60), ignore_exceptions=True),  # 运行60秒
                fetcher.fetch_stock_list('all'),
                fetcher.fetch_historical_data('000001', '2023-01-01', '2023-12-31', klt='101', fqt='0'),
                fetcher.fetch_dividend_data('000001'),
                fetcher.fetch_financial_data('000001', report_type='annual'),
            ]
            await asyncio.gather(*tasks)
    
    # 运行异步函数
    asyncio.run(main())