import asyncio
import json
import time
from datetime import datetime, timedelta
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
    timestamp: datetime # 数据时间戳


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
                timestamp=datetime.strptime(f"{fields[30]} {fields[31]}", "%Y-%m-%d %H:%M:%S")  # 行情时间
            )
            quotes.append(quote)
        
        logging.info(f"Fetched {len(quotes)} realtime quotes for symbols: {', '.join(symbols)}, detail info: {', '.join([f'{q.symbol}: {q.price} ({q.change_percent:.2f}%)' for q in quotes])}")
        return quotes

    @retry(max_retries=3, delay=1)
    def fetch_historical_data(self, symbol: str, start_date: str, end_date: str) -> List[HistoricalData]:
        """
        从东方财富获取历史行情数据
        
        Args:
            symbol: 股票代码，如'000001'
            start_date: 开始日期，格式'YYYY-MM-DD'
            end_date: 结束日期，格式'YYYY-MM-DD'
        
        Returns:
            历史行情数据列表
        """
        self.rate_limiter.acquire()
        
        # 转换股票代码格式
        if symbol.startswith('6'):
            secid = f'1.{symbol}'  # 沪市
        else:
            secid = f'0.{symbol}'  # 深市
        
        # 东方财富历史数据API
        # 参数说明：
        # secid: 证券ID，格式为市场代码.股票代码
        # klt: K线类型，101=日K线
        # fqt: 复权类型，1=前复权，2=后复权，0=不复权
        # beg: 开始日期
        # end: 结束日期
        params = {
            'secid': secid,
            'klt': '101',  # 日K线
            'fqt': '1',    # 前复权
            'beg': start_date.replace('-', ''),
            'end': end_date.replace('-', ''),
            'fields1': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        }
        
        url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?{urlencode(params)}"
        
        response = self.spider_core.get(url, headers=self.eastmoney_headers)
        
        if not response or response.status_code != 200:
            raise Exception(f"Failed to fetch historical data for {symbol}: {response.status_code if response else 'No response'}")
        
        data = response.json()
        
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
        
        return historical_data

    @retry(max_retries=3, delay=1)
    def fetch_stock_list(self, market: str = 'all') -> List[StockInfo]:
        """
        从东方财富获取股票列表
        
        Args:
            market: 市场类型，'sh'=沪市，'sz'=深市，'all'=全部
        
        Returns:
            股票信息列表
        """
        self.rate_limiter.acquire()
        
        # 东方财富股票列表API
        # 参数说明：
        # pz: 每页数量
        # pn: 页码
        # po: 排序方式
        # fields: 返回字段
        params = {
            'pz': '5000',  # 每页5000条
            'pn': '1',     # 第1页
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
        
        response = self.spider_core.get(url, headers=self.eastmoney_headers)
        
        if not response or response.status_code != 200:
            raise Exception(f"Failed to fetch stock list: {response.status_code if response else 'No response'}")
        
        data = response.json()
        
        if data['rc'] != 0 or not data.get('data', {}).get('diff'):
            raise Exception("No stock list data found")
        
        stocks = []
        for item in data['data']['diff']:
            symbol = item['f12']
            market_code = 'SH' if symbol.startswith('6') else 'SZ'
            
            stocks.append(StockInfo(
                symbol=symbol,
                name=item['f14'],
                market=market_code,
                industry=item.get('f13', ''),
                list_date=''  # 此API不返回上市日期
            ))
        
        return stocks

    @retry(max_retries=3, delay=1)
    def fetch_financial_data(self, symbol: str, report_type: str = 'annual') -> List[FinancialData]:
        """
        从东方财富获取财务数据
        
        Args:
            symbol: 股票代码
            report_type: 报告类型，'annual'=年报，'quarterly'=季报
        
        Returns:
            财务数据列表
        """
        self.rate_limiter.acquire()
        
        # 转换股票代码格式
        if symbol.startswith('6'):
            secid = f'1.{symbol}'
        else:
            secid = f'0.{symbol}'
        
        # 东方财富财务数据API
        # 参数说明：
        # type: 报表类型，4=利润表
        # rpttype: 报告期类型，4=年报，1=季报
        params = {
            'type': '4',  # 利润表
            'rpttype': '4' if report_type == 'annual' else '1',
            'secid': secid,
            'companytype': '4',
        }
        
        url = f"https://emh5.eastmoney.com/api/CaiWuFenXi/GetCaiWuFenXi?{urlencode(params)}"
        
        response = self.spider_core.get(url, headers=self.eastmoney_headers)
        
        if not response or response.status_code != 200:
            raise Exception(f"Failed to fetch financial data for {symbol}: {response.status_code if response else 'No response'}")
        
        data = response.json()
        
        if not data.get('Result'):
            raise Exception(f"No financial data found for {symbol}")
        
        financial_data = []
        for item in data['Result']:
            financial_data.append(FinancialData(
                symbol=symbol,
                report_date=item.get('REPORTDATE', ''),
                revenue=float(item.get('TOTALOPERATEREVE', 0)),
                net_profit=float(item.get('NETPROFIT', 0)),
                eps=float(item.get('BASICEPS', 0)),
                roe=float(item.get('ROEJQ', 0)),
                total_assets=float(item.get('TOTALASSETS', 0)),
                total_liabilities=float(item.get('TOTALLIAB', 0))
            ))
        
        return financial_data

    @retry(max_retries=3, delay=1)
    def fetch_dividend_data(self, symbol: str) -> List[DividendData]:
        """
        从东方财富获取除权除息数据
        
        Args:
            symbol: 股票代码
        
        Returns:
            除权除息数据列表
        """
        self.rate_limiter.acquire()
        
        # 转换股票代码格式
        if symbol.startswith('6'):
            secid = f'1.{symbol}'
        else:
            secid = f'0.{symbol}'
        
        # 东方财富除权除息API
        # 参数说明：
        # secid: 证券ID
        params = {
            'secid': secid,
        }
        
        url = f"https://emh5.eastmoney.com/api/FenHong/GetFenHongSong?{urlencode(params)}"
        
        response = self.spider_core.get(url, headers=self.eastmoney_headers)
        
        if not response or response.status_code != 200:
            raise Exception(f"Failed to fetch dividend data for {symbol}: {response.status_code if response else 'No response'}")
        
        data = response.json()
        
        if not data.get('Result'):
            raise Exception(f"No dividend data found for {symbol}")
        
        dividend_data = []
        for item in data['Result']:
            dividend_data.append(DividendData(
                symbol=symbol,
                ex_date=item.get('CQCXR', ''),
                dividend_per_share=float(item.get('FHPS', 0)),
                bonus_share_ratio=float(item.get('SGBL', 0)),
                rights_issue_ratio=float(item.get('PGBL', 0)),
                rights_issue_price=float(item.get('PGJ', 0))
            ))
        
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
            ]
            await asyncio.gather(*tasks)
    
    # 运行异步函数
    asyncio.run(main())