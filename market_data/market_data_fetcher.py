import asyncio
import json
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from urllib.parse import urlencode
import logging
import os
from contextlib import ExitStack, AsyncExitStack
from ..spider.rate_limiter import RateLimiter, RateLimiterManager
from ..spider.spider_core import AntiDetectionSpider
from ..utils.retry import async_retry
from ..utils.bytes_str_convert import from_bytes_to_str, from_str_to_bytes
from ..utils.call_loop import async_call_loop
from ..utils.parse_html_elem import extract_content
from ..dao.csv_dao import CSVGenericDAO

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

    # 买1-5数据
    buy1_price: float
    buy1_volume: int
    buy2_price: float
    buy2_volume: int
    buy3_price: float
    buy3_volume: int
    buy4_price: float
    buy4_volume: int
    buy5_price: float
    buy5_volume: int

    # 卖1-5数据
    sell1_price: float
    sell1_volume: int
    sell2_price: float
    sell2_volume: int
    sell3_price: float
    sell3_volume: int
    sell4_price: float
    sell4_volume: int
    sell5_price: float
    sell5_volume: int


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


@dataclass
class FinancialData:
    """财务数据结构，包含利润表、资产负债表和现金流量表的重要字段"""
    symbol: str
    report_date: str

    # 利润表
    total_revenue: float             # 营业收入（TOTAL_OPERATE_INCOME）
    operating_cost: float            # 营业成本（OPERATE_COST）
    gross_profit: float              # 毛利润（GROSS_PROFIT）
    operating_profit: float          # 营业利润（OPERATE_PROFIT）
    profit_before_tax: float         # 利润总额（TOTAL_PROFIT）
    net_profit: float                # 归属母公司所有者的净利润（PARENT_NETPROFIT）
    eps: float                        # 每股收益（BASIC_EPS）
    roe: float                        # 净资产收益率（WEIGHTAVG_ROE）

    # 资产负债表
    total_assets: float              # 资产总计（TOTAL_ASSETS）
    current_assets: float            # 流动资产合计（CURRENT_ASSETS）
    non_current_assets: float        # 非流动资产合计（NON_CURRENT_ASSETS）
    total_liabilities: float         # 负债合计（TOTAL_LIABILITIES）
    current_liabilities: float       # 流动负债合计（CURRENT_LIABILITIES）
    non_current_liabilities: float   # 非流动负债合计（NON_CURRENT_LIABILITIES）
    total_equity: float              # 股东权益合计（TOTAL_EQUITY）

    # 现金流量表
    net_operate_cashflow: float      # 经营活动产生的现金流量净额（NET_CASH_OPER_ACT）
    net_invest_cashflow: float       # 投资活动产生的现金流量净额（NET_CASH_INVEST_ACT）
    net_finance_cashflow: float      # 筹资活动产生的现金流量净额（NET_CASH_FINA_ACT）
    free_cashflow: float             # 自由现金流（FREE_CASH_FLOW）


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

    @async_retry(max_retries=3, delay=1)
    async def fetch_realtime_quotes(self, symbols: List[str], csv_dao: CSVGenericDAO[RealTimeQuote]) -> List[RealTimeQuote]:
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
            code, market = symbol.split('.')
            if market.upper() not in ['SH', 'SZ', 'BJ']:
                raise Exception(f"Unsupported market type: {market}. Expected 'SH', 'SZ', or 'BJ'.")
            if market.upper() == 'SH':
                sina_symbols.append(f"sh{code}")
            elif market.upper() == 'SZ':
                sina_symbols.append(f"sz{code}")
            else: # market.upper() == 'BJ'
                sina_symbols.append(f"bj{code}")
        
        # 新浪实时行情API：返回JavaScript格式数据
        # 参数：list为股票代码，用逗号分隔
        url = f"https://hq.sinajs.cn/list={','.join(sina_symbols)}"

        async with self.rate_limiter_mgr.get_rate_limiter('hq.sinajs.cn'):
            response = await self.spider.crawl_url(url, headers=self.sina_headers)
        
        if not response or not response.success:
            raise Exception(f"Failed to fetch realtime quotes: {response.error if response else 'No response'}")
        
        quotes = []
        lines = extract_content(response.content, "html > body > pre").split('\n')

        for i, line in enumerate(lines):
            if i >= len(symbols):
                break

            if '=' not in line:
                raise Exception(f"Invalid data format in line {i + 1}: {line}")
                
            # 解析新浪返回的数据格式
            # var hq_str_sh600000="浦发银行,14.170,14.200,13.800,14.240,13.800,13.800,13.810,161683677,2258575044.000,354522,13.800,154900,13.790,393300,13.780,106500,13.770,79700,13.760,1000,13.810,700,13.820,6000,13.830,9700,13.840,52300,13.850,2025-07-11,15:00:03,00,"
            left_part, right_part = line.split('=')
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
                timestamp=f"{fields[30]} {fields[31]}",  # 行情时间

                # 买1-5数据
                buy1_price=float(fields[11]),
                buy1_volume=int(fields[10]),
                buy2_price=float(fields[13]),
                buy2_volume=int(fields[12]),
                buy3_price=float(fields[15]),
                buy3_volume=int(fields[14]),
                buy4_price=float(fields[17]),
                buy4_volume=int(fields[16]),
                buy5_price=float(fields[19]),
                buy5_volume=int(fields[18]),

                # 卖1-5数据
                sell1_price=float(fields[21]),
                sell1_volume=int(fields[20]),
                sell2_price=float(fields[23]),
                sell2_volume=int(fields[22]),
                sell3_price=float(fields[25]),
                sell3_volume=int(fields[24]),
                sell4_price=float(fields[27]),
                sell4_volume=int(fields[26]),
                sell5_price=float(fields[29]),
                sell5_volume=int(fields[28]),
            )
            quotes.append(quote)
        
        logging.info(f"Fetched {len(quotes)} realtime quotes for symbols: {', '.join(symbols)}, detail info: {', '.join([f'{q.symbol}: {q.price} ({q.change_percent:.2f}%)' for q in quotes])}")

        csv_dao.write_records(quotes)
        return quotes

    @async_retry(max_retries=3, delay=1)
    async def fetch_historical_data(self, symbol: str, start_date: str, end_date: str, csv_dao: CSVGenericDAO[HistoricalData], klt: str='101', fqt: str='0') -> List[HistoricalData]:
        """
        从东方财富获取历史行情数据
        
        Args:
            symbol: 股票代码，如'000001.SZ' 或 '600000.SH'
            start_date: 开始日期，格式'YYYY-MM-DD'
            end_date: 结束日期，格式'YYYY-MM-DD'
        
        Returns:
            历史行情数据列表
        """

        # 转换股票代码格式
        code, market = symbol.split('.')
        if market.upper() not in ['SH', 'SZ', 'BJ']:
            raise Exception(f"Unsupported market type: {market}. Expected 'SH' or 'SZ' or 'BJ'.")
        if market.upper() == 'SH':
            secid = f'1.{code}'
        else: # market.upper() == 'SZ' or market.upper() == 'BJ'
            secid = f'0.{code}'
        
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
            response = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
        
        if not response or not response.success:
            raise Exception(f"Failed to fetch historical data for {symbol}: {response.error if response else 'No response'}")

        data = json.loads(extract_content(response.content, "html > body > pre"))

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

        csv_dao.write_records(historical_data)
        return historical_data

    @async_retry(max_retries=3, delay=1)
    async def fetch_stock_list(self, csv_dao: CSVGenericDAO[StockInfo]) -> List[StockInfo]:
        """
        从东方财富新版 push2delay 接口获取股票列表
        
        Returns:
            股票信息列表
        """
        all_stocks: List[StockInfo] = []
        page = 1
        page_size = 100

        # 默认 fs 参数：全部市场
        sh_a = 'm:1+t:2,m:1+t:23'
        sz_a = 'm:0+t:6,m:0+t:80'
        bj_a = 'm:0+t:81+s:2048'
        markets = {
            'SH': sh_a,
            'SZ': sz_a,
            'BJ': bj_a,
        }
        
        for market, fs in markets.items():
            while True:
                params = {
                    'np':    '1',
                    'fltt':  '1',
                    'invt':  '2',
                    'fs':    fs,
                    'fields':'f12,f13,f14,f1,f2,f4,f3,f152,f5,f6,f7,f15,f18,f16,f17,f10,f8,f9,f23',
                    'fid':   'f3',
                    'pn':    str(page),
                    'pz':    str(page_size),
                    'po':    '1',
                    'dect':  '1',
                }
                url = f"https://push2delay.eastmoney.com/api/qt/clist/get?{urlencode(params)}"
                async with self.rate_limiter_mgr.get_rate_limiter('push2delay.eastmoney.com'):
                    response = await self.spider.crawl_url(url, headers=self.eastmoney_headers)

                if not response or not response.success:
                    raise Exception(f"Failed to fetch stock list: {response.error if response else 'No response'}")

                payload = json.loads(extract_content(response.content, "html > body > pre"))
                diff = payload.get('data', {}).get('diff')
                if not diff:
                    break

                page_stocks: List[StockInfo] = []
                for rec in diff:
                    code = rec.get('f12', '')
                    name = rec.get('f14', '')
                    page_stocks.append(StockInfo(
                        symbol=f"{code}.{market}",
                        name=name,
                        market=market,
                    ))

                all_stocks.extend(page_stocks)
                if len(page_stocks) < page_size:
                    break
                page += 1

        logging.info(f"Fetched {len(all_stocks)} stocks for market: {market}")
        csv_dao.write_records(all_stocks)
        return all_stocks

    @async_retry(max_retries=3, delay=1)
    async def fetch_financial_data(self, symbol: str, csv_dao: CSVGenericDAO[FinancialData]) -> List[FinancialData]:
        # 从东方财富新版接口获取财务三表（利润表、资产负债表、现金流量表）并按 REPORT_DATE 合并
        
        # 示例接口：
        # 现金流量表 (type=RPT_F10_FINANCE_BCASHFLOW, sty=APP_F10_BCASHFLOW)
        # GET https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_F10_FINANCE_BCASHFLOW&sty=APP_F10_BCASHFLOW&filter=(SECUCODE="000001.SZ")&p=1&ps=50&sr=-1&st=REPORT_DATE&source=HSF10&client=PC
        # 返回字段 NETCASH_OPERATE, NETCASH_INVEST, NETCASH_FINANCE, CCE_ADD
        
        # 利润表 (type=RPT_F10_FINANCE_BINCOME, sty=APP_F10_BINCOME)
        # GET https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_F10_FINANCE_BINCOME&sty=APP_F10_BINCOME&filter=(SECUCODE="000001.SZ")&p=1&ps=50&sr=-1&st=REPORT_DATE&source=HSF10&client=PC
        # 返回字段 OPERATE_INCOME, OPERATE_EXPENSE, OPERATE_PROFIT, TOTAL_PROFIT, PARENT_NETPROFIT, BASIC_EPS
        
        # 资产负债表 (type=RPT_F10_FINANCE_BBALANCE, sty=F10_FINANCE_BBALANCE)
        # GET https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_F10_FINANCE_BBALANCE&sty=F10_FINANCE_BBALANCE&filter=(SECUCODE="000001.SZ")&p=1&ps=50&sr=-1&st=REPORT_DATE&source=HSF10&client=PC
        # 返回字段 TOTAL_ASSETS, TOTAL_LIABILITIES, TOTAL_PARENT_EQUITY
        
        tables = [
            ("RPT_F10_FINANCE_BCASHFLOW", "APP_F10_BCASHFLOW"),
            ("RPT_F10_FINANCE_BINCOME",    "APP_F10_BINCOME"),
            ("RPT_F10_FINANCE_BBALANCE",   "F10_FINANCE_BBALANCE"),
        ]
        records: Dict[str, Dict[str, Any]] = {}
        page_size = 50
        
        for t, sty in tables:
            page = 1
            while True:
                params = {
                    "type":  t,
                    "sty":   sty,
                    "filter": f'(SECUCODE="{symbol}")',
                    "p":      page,
                    "ps":     page_size,
                    "sr":    -1,
                    "st":   "REPORT_DATE",
                    "source":"HSF10",
                    "client":"PC",
                }
                url = f"https://datacenter.eastmoney.com/securities/api/data/get?{urlencode(params)}"
                async with self.rate_limiter_mgr.get_rate_limiter("datacenter.eastmoney.com"):
                    resp = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
                if not resp or not resp.success:
                    raise Exception(f"Failed to fetch {t} for {symbol}: {resp.error if resp else 'No response'}")
                
                payload = json.loads(extract_content(resp.content, "html > body > pre"))
                rows = payload.get('result', {}).get("data", [])
                if not rows:
                    break
                
                for itm in rows:
                    rpt = itm.get("REPORT_DATE", "").split(" ")[0]
                    rec = records.setdefault(rpt, {"symbol": symbol, "report_date": rpt})
                    if t == "RPT_F10_FINANCE_BCASHFLOW":
                        rec["net_operate_cashflow"] = float(itm.get("NETCASH_OPERATE") or 0)
                        rec["net_invest_cashflow"] = float(itm.get("NETCASH_INVEST") or 0)
                        rec["net_finance_cashflow"] = float(itm.get("NETCASH_FINANCE") or 0)
                        rec["free_cashflow"]      = float(itm.get("CCE_ADD") or 0)
                    elif t == "RPT_F10_FINANCE_BINCOME":
                        rec["total_revenue"]   = float(itm.get("OPERATE_INCOME") or 0)
                        rec["operating_cost"]  = float(itm.get("OPERATE_EXPENSE") or 0)
                        rec["operating_profit"]= float(itm.get("OPERATE_PROFIT") or 0)
                        rec["profit_before_tax"]= float(itm.get("TOTAL_PROFIT") or 0)
                        rec["net_profit"]      = float(itm.get("PARENT_NETPROFIT") or 0)
                        rec["eps"]             = float(itm.get("BASIC_EPS") or 0)
                        # 计算毛利
                        rec["gross_profit"] = rec["total_revenue"] - rec["operating_cost"]
                    else:  # BBALANCE
                        rec["total_assets"]        = float(itm.get("TOTAL_ASSETS") or 0)
                        rec["total_liabilities"]   = float(itm.get("TOTAL_LIABILITIES") or 0)
                        rec["total_equity"]        = float(itm.get("TOTAL_PARENT_EQUITY") or 0)
                        # 其他资产负债细分留空
                if len(rows) < page_size:
                    break
                page += 1
        
        # 构建 dataclass 列表
        result: List[FinancialData] = []
        for rpt, rec in sorted(records.items(), reverse=True):
            result.append(
                FinancialData(
                    symbol                 = rec["symbol"],
                    report_date            = rec["report_date"],
                    total_revenue          = rec.get("total_revenue", 0.0),
                    operating_cost         = rec.get("operating_cost", 0.0),
                    gross_profit           = rec.get("gross_profit", 0.0),
                    operating_profit       = rec.get("operating_profit", 0.0),
                    profit_before_tax      = rec.get("profit_before_tax", 0.0),
                    net_profit             = rec.get("net_profit", 0.0),
                    eps                    = rec.get("eps", 0.0),
                    roe                    = (rec.get("net_profit", 0.0) / rec.get("total_equity", 1.0)) * 100 if rec.get("total_equity", 0.0) > 0 else 0.0,
                    total_assets           = rec.get("total_assets", 0.0),
                    current_assets         = rec.get("current_assets", 0.0),
                    non_current_assets     = rec.get("non_current_assets", 0.0),
                    total_liabilities      = rec.get("total_liabilities", 0.0),
                    current_liabilities    = rec.get("current_liabilities", 0.0),
                    non_current_liabilities= rec.get("non_current_liabilities", 0.0),
                    total_equity           = rec.get("total_equity", 0.0),
                    net_operate_cashflow   = rec.get("net_operate_cashflow", 0.0),
                    net_invest_cashflow    = rec.get("net_invest_cashflow", 0.0),
                    net_finance_cashflow   = rec.get("net_finance_cashflow", 0.0),
                    free_cashflow          = rec.get("free_cashflow", 0.0),
                )
            )
        csv_dao.write_records(result)
        return result

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
        
        with ExitStack() as stack:
            async with AsyncExitStack() as async_stack:
                spider = await async_stack.enter_async_context(AntiDetectionSpider())
                fetcher = MarketDataFetcher(rate_limiter_mgr, spider)

                # 创建定时器函数，运行指定时间后返回False
                def create_timer_check_func(duration_seconds: int):
                    start_time = time.time()
                    def check_func():
                        return time.time() - start_time < duration_seconds
                    return check_func

                os.remove('realtime_quotes.csv') if os.path.exists('realtime_quotes.csv') else None
                os.remove('historical_data.csv') if os.path.exists('historical_data.csv') else None
                os.remove('stock_list.csv') if os.path.exists('stock_list.csv') else None
                os.remove('financial_data.csv') if os.path.exists('financial_data.csv') else None
                os.remove('dividend_data.csv') if os.path.exists('dividend_data.csv') else None

                realtime_quote_csv_dao = stack.enter_context(CSVGenericDAO('realtime_quotes.csv', RealTimeQuote))
                historical_data_csv_dao = stack.enter_context(CSVGenericDAO('historical_data.csv', HistoricalData))
                stock_info_csv_dao = stack.enter_context(CSVGenericDAO('stock_list.csv', StockInfo))
                financial_data_csv_dao = stack.enter_context(CSVGenericDAO('financial_data.csv', FinancialData))

                tasks = [
                    async_call_loop(fetcher.fetch_realtime_quotes, ['000001', '000002'], realtime_quote_csv_dao, interval=1.0, check_func=create_timer_check_func(30), ignore_exceptions=True),  # 运行30秒
                    #fetcher.fetch_historical_data('000001', '2025-01-01', '2025-07-13', historical_data_csv_dao, klt='101', fqt='0'),
                    #fetcher.fetch_stock_list(stock_info_csv_dao, 'all'),
                    #fetcher.fetch_financial_data('000001', 'SZ', financial_data_csv_dao),
                ]
                await asyncio.gather(*tasks)

    # 运行异步函数
    asyncio.run(main())