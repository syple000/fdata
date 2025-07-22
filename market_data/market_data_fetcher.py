import json
import time
from typing import Dict, List, Any
from dataclasses import asdict
from urllib.parse import urlencode
import logging
import asyncio
from ..spider.rate_limiter import RateLimiterManager
from ..spider.spider_core import AntiDetectionSpider
from ..utils.retry import async_retry
from ..utils.parse_html_elem import extract_content
from ..dao.csv_dao import CSVGenericDAO
from .models import *
from .market_stock_list_fs import MARKET_STOCK_LIST_FS

class MarketDataFetcher:
    """市场数据获取器"""
    def __init__(self, rate_limiter_mgr: RateLimiterManager, spider: AntiDetectionSpider):
        self.rate_limiter_mgr = rate_limiter_mgr
        self.spider = spider
        
        # 新浪财经实时行情headers
        self.sina_headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://finance.sina.com.cn/stock/",
            "Sec-Fetch-Dest": "script",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Storage-Access": "active",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        
        # 东方财富headers
        self.eastmoney_headers = {
            "sec-ch-ua-platform": "\"Windows\"",
            "Referer": "http://quote.eastmoney.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
            "sec-ch-ua-mobile": "?0"
        }
    
    async def fetch_realtime_quotes(self, symbols: List[Symbol], csv_dao: CSVGenericDAO[RealTimeQuote], from_: str = 'sina') -> List[RealTimeQuote]:
        if from_ == 'sina':
            return await self._fetch_realtime_quotes_sina(symbols, csv_dao)
        elif from_ == 'eastmoney':
            raise NotImplementedError("Eastmoney real-time quotes fetching is not implemented yet.")
        else:
            raise ValueError(f"Unsupported source: {from_}. Supported sources are 'sina' and 'eastmoney'.")

    @async_retry(max_retries=1, delay=0, ignore_exceptions=True)
    async def _fetch_realtime_quotes_sina(self, symbols: List[Symbol], csv_dao: CSVGenericDAO[RealTimeQuote]) -> List[RealTimeQuote]:
        """
        从新浪财经获取实时行情，支持股票和指数
        
        Returns:
            实时行情数据列表
        """
        
        sina_symbols = []
        for symbol in symbols:
            if symbol.type == Type.INDEX.value:
                sina_symbols.append(f"s_{symbol.market.lower()}{symbol.code}")
            elif symbol.type == Type.STOCK.value:
                sina_symbols.append(f"{symbol.market.lower()}{symbol.code}")
            else:
                raise Exception(f"Unsupported symbol type: {symbol.type}. Only STOCK and INDEX are supported.")
        
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
            if symbols[i].code not in left_part:
                raise Exception(f"Symbol mismatch: {symbols[i]} not found in {left_part}")

            fields = right_part.strip('";\n').split(',')

            if symbols[i].type == Type.INDEX.value:
                if len(fields) < 6:
                    raise Exception(f"Insufficient data fields for index {symbols[i]}: {fields}")
                # 指数数据格式：名称,当前价格,涨跌额,涨跌幅,成交量,成交额
                quote = RealTimeQuote(
                    symbol=symbols[i],
                    name=fields[0],                    # 指数名称
                    price=float(fields[1]),            # 当前价格
                    change=float(fields[2]),           # 涨跌额
                    change_percent=float(fields[3]),   # 涨跌幅
                    volume=int(fields[4]),             # 成交量
                    turnover=float(fields[5]),         # 成交额
                    open_price=0.0,                    # 指数无开盘价
                    high_price=0.0,                    # 指数无最高价
                    low_price=0.0,                     # 指数无最低价
                    prev_close=float(fields[1]) - float(fields[2]),  # 昨收价 = 当前价 - 涨跌额
                    timestamp="",                      # 指数无具体时间戳

                    # 指数无买卖盘数据
                    buy1_price=0.0, buy1_volume=0,
                    buy2_price=0.0, buy2_volume=0,
                    buy3_price=0.0, buy3_volume=0,
                    buy4_price=0.0, buy4_volume=0,
                    buy5_price=0.0, buy5_volume=0,
                    sell1_price=0.0, sell1_volume=0,
                    sell2_price=0.0, sell2_volume=0,
                    sell3_price=0.0, sell3_volume=0,
                    sell4_price=0.0, sell4_volume=0,
                    sell5_price=0.0, sell5_volume=0,
                )
                quotes.append(quote)
            elif symbols[i].type == Type.STOCK.value:
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
            else:
                raise Exception(f"Unsupported symbol type: {symbols[i].type}. Only STOCK and INDEX are supported.")
        
        logging.info(f"Fetched {len(quotes)} realtime quotes for symbols: {', '.join([x.code + '.' + x.market for x in symbols])}, detail info: {', '.join([f'{q.symbol}: {q.price} ({q.change_percent:.2f}%)' for q in quotes])}")

        csv_dao.write_records(quotes)
        return quotes

    async def fetch_historical_data(self, symbol: Symbol, start_date: str, end_date: str, csv_dao: CSVGenericDAO[HistoricalData], klt: KLineType=KLineType.DAILY, fqt: AdjustType=AdjustType.NONE, from_: str='eastmoney') -> List[HistoricalData]:
        if from_ == 'eastmoney':
            return await self._fetch_historical_data_em(symbol, start_date, end_date, csv_dao, klt, fqt)
        elif from_ == 'sina': # 仅支持分钟线未复权数据
            return await self._fetch_historical_data_sina(symbol, csv_dao, klt)
        else:
            raise ValueError(f"Unsupported source: {from_}. Supported sources are 'eastmoney' and 'sina'.")

    @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
    async def _fetch_historical_data_sina(self, symbol: Symbol, csv_dao: CSVGenericDAO[HistoricalData], klt: KLineType) -> List[HistoricalData]:
        """
        从新浪获取股票和指数历史行情数据（5/15/30/60 min数据，未复权）
        
        Args:
            symbol: symbol
            klt: K线类型
        
        Returns:
            未复权历史分钟行情数据列表
        """
        
        # 转换symbol格式
        if symbol.type == Type.INDEX.value:
            sina_symbol = f"{symbol.market.lower()}{symbol.code}"
        elif symbol.type == Type.STOCK.value:
            sina_symbol = f"{symbol.market.lower()}{symbol.code}"
        else:
            raise Exception(f"Unsupported symbol type: {symbol.type}. Only STOCK and INDEX are supported.")
        
        # K线类型映射
        scale_map = {
            KLineType.MIN5: '5',
            KLineType.MIN15: '15', 
            KLineType.MIN30: '30',
            KLineType.MIN60: '60',
        }
        if klt not in scale_map:
            raise Exception(f"Unsupported KLineType: {klt}. Supported types are: {', '.join(scale_map.keys())}")
        
        scale = scale_map.get(klt)
        
        # 新浪历史数据API
        params = {
            'symbol': sina_symbol,
            'scale': scale,
            'ma': 'no',
            'datalen': '1960'  # 获取最近1960条数据
        }
        
        # 生成随机回调函数名
        callback_name = f"var _{sina_symbol}_{scale}_{int(time.time() * 1000)}"
        
        url = f"https://quotes.sina.cn/cn/api/jsonp_v2.php/{callback_name}=/CN_MarketDataService.getKLineData"
        full_url = f"{url}?{urlencode(params)}"
        
        logging.info(f"Fetching historical data for {symbol} from Sina, URL: {full_url}")
        
        async with self.rate_limiter_mgr.get_rate_limiter('quotes.sina.cn'):
            response = await self.spider.crawl_url(full_url, headers=self.sina_headers)
        
        if not response or not response.success:
            raise Exception(f"Failed to fetch historical data for {symbol}: {response.error if response else 'No response'}")

        content = extract_content(response.content, "html > body > pre")
        
        # 解析JSONP格式数据
        # 格式: var _callback_name=([{...}]);
        # 跳过首行/*<script>location.href='//sina.com';</script>*/
        if content.startswith('/*<script>'):
            content = content.split('*/', 1)[1].strip()
        # 找到 = 后面的JSON数组部分
        if '=' not in content:
            raise Exception(f"Invalid JSONP format: {content[:100]}...")
        
        json_part = content.split('=', 1)[1].strip()
        if json_part.endswith(');'):
            json_part = json_part[:-2]  # 移除 );
        elif json_part.endswith(')'):
            json_part = json_part[:-1]  # 移除 )
        
        # 如果以 ( 开头，移除它
        if json_part.startswith('('):
            json_part = json_part[1:]
        
        data = json.loads(json_part)
        
        historical_data = []
        for item in data:
            # 数据格式：{"day":"2025-07-18 15:00:00","open":"3535.480","high":"3535.703","low":"3534.483","close":"3534.483","volume":"1455987400","amount":"17764974592.0000"}
            date_str = item['day']
            open_price = float(item['open'])
            high_price = float(item['high'])
            low_price = float(item['low'])
            close_price = float(item['close'])
            volume = int(int(item['volume'])/100)
            turnover = 0  # 新浪不提供成交数据
            change_percent = 0.0 # 新浪不提供涨跌幅数据
            
            historical_data.append(HistoricalData(
                symbol=symbol,
                date=date_str,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume=volume,
                turnover=turnover,
                change_percent=change_percent
            ))
        
        # 按日期排序
        historical_data.sort(key=lambda x: x.date)
        
        csv_dao.write_records(historical_data)
        return historical_data

    @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
    async def _fetch_historical_data_em(self, symbol: Symbol, start_date: str, end_date: str, csv_dao: CSVGenericDAO[HistoricalData], klt: KLineType=KLineType.DAILY, fqt: AdjustType=AdjustType.NONE) -> List[HistoricalData]:
        """
        从东方财富获取股票和指数历史行情数据
        
        Args:
            symbol: symbol
            start_date: 开始日期，格式'YYYY-MM-DD'
            end_date: 结束日期，格式'YYYY-MM-DD'
        
        Returns:
            历史行情数据列表
        """

        # 转换股票代码格式
        if symbol.market == MarketType.SH.value:
            secid = f'1.{symbol.code}'
        elif symbol.market in [MarketType.SZ.value, MarketType.BJ.value]:
            secid = f'0.{symbol.code}'
        else:
            raise Exception(f"Unsupported market type: {symbol.market}. Expected 'SH', 'SZ' or 'BJ'.")
        
        # 东方财富历史数据API
        # 参数说明：
        # secid: 证券ID，格式为市场代码.股票代码
        # klt: K线类型，101=日K线，102=周K线，103=月K线，5=5分钟，15=15分钟，30=30分钟，60=60分钟
        # fqt: 复权类型，1=前复权，2=后复权，0=不复权
        # beg: 开始日期
        # end: 结束日期
        params = {
            'secid': secid,
            'klt': klt.value,  # 日K线
            'fqt': fqt.value,    # 前复权
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

        if data['rc'] != 0 or not data['data'] or not data['data']['klines']:
            return []
        
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
    
    async def fetch_stock_quote(self, symbol: Symbol, csv_dao: CSVGenericDAO[StockQuoteInfo], from_: str = 'eastmoney') -> StockQuoteInfo:
        if from_ == 'eastmoney':
            return await self._fetch_stock_quote_em(symbol, csv_dao)
        elif from_ == 'sina':
            raise NotImplementedError("Sina stock quote fetching is not implemented yet.")
        else:
            raise ValueError(f"Unsupported source: {from_}. Supported sources are 'eastmoney' and 'sina'.")

    @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
    async def _fetch_stock_quote_em(self, symbol: Symbol, csv_dao: CSVGenericDAO[StockQuoteInfo]) -> StockQuoteInfo:
        """
        从东方财富获取股票详细quote信息
        
        Returns:
            股票quote信息
        """
        
        # 转换股票代码格式
        if symbol.market == MarketType.SH.value:
            secid = f'1.{symbol.code}'
        elif symbol.market in [MarketType.SZ.value, MarketType.BJ.value]:
            secid = f'0.{symbol.code}'
        else:
            raise Exception(f"Unsupported market type: {symbol.market}. Expected 'SH', 'SZ' or 'BJ'.")
        
        params = {
            'invt': '2',
            'fltt': '1',
            'fields': 'f58,f734,f107,f57,f43,f59,f169,f301,f60,f170,f152,f177,f111,f46,f44,f45,f47,f260,f48,f261,f279,f277,f278,f288,f19,f17,f531,f15,f13,f11,f20,f18,f16,f14,f12,f39,f37,f35,f33,f31,f40,f38,f36,f34,f32,f211,f212,f213,f214,f215,f210,f209,f208,f207,f206,f161,f49,f171,f50,f86,f84,f85,f168,f108,f116,f167,f164,f162,f163,f92,f71,f117,f292,f51,f52,f191,f192,f262,f294,f295,f269,f270,f256,f257,f285,f286,f748,f747',
            'secid': secid
        }
        
        url = f"https://push2delay.eastmoney.com/api/qt/stock/get?{urlencode(params)}"
        logging.info(f"Fetching stock quote for {symbol}, URL: {url}")
        
        async with self.rate_limiter_mgr.get_rate_limiter('push2delay.eastmoney.com'):
            response = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
        
        if not response or not response.success:
            raise Exception(f"Failed to fetch stock quote for {symbol}: {response.error if response else 'No response'}")

        payload = json.loads(extract_content(response.content, "html > body > pre"))
        
        if payload['rc'] != 0 or not payload['data']:
            raise Exception(f"Invalid response for stock quote {symbol}: {payload}")
        
        data = payload['data']
        
        quote_info = StockQuoteInfo(
            symbol=symbol,
            name=data.get('f58', ''),                    # 股票名称
            open_price=data.get('f46', 0) / 100.0,       # 今开
            prev_close=data.get('f60', 0) / 100.0,       # 昨收
            high_price=data.get('f44', 0) / 100.0,       # 最高
            low_price=data.get('f45', 0) / 100.0,        # 最低
            limit_up=data.get('f51', 0) / 100.0,         # 涨停
            limit_down=data.get('f52', 0) / 100.0,       # 跌停
            turnover_rate=data.get('f168', 0) / 100.0,   # 换手率
            volume_ratio=data.get('f50', 0) / 100.0,     # 量比
            volume=data.get('f47', 0),                   # 成交量
            turnover=data.get('f48', 0),                 # 成交额
            pe_dynamic=data.get('f162', 0) / 100.0,      # 市盈率(动)
            pe_lyr=data.get('f163', 0) / 100.0,        # 市盈率(静态)
            pe_ttm=data.get('f164', 0) / 100.0,        # 市盈率(TTM)
            pb_ratio=data.get('f167', 0) / 100.0,        # 市净率
            total_market_cap=data.get('f116', 0),      # 总市值
            circulating_market_cap=data.get('f117', 0) # 流通市值
        )
        
        logging.info(f"Fetched stock quote for {symbol}: {quote_info.name}, open: {quote_info.open_price}, high: {quote_info.high_price}, low: {quote_info.low_price}")
        
        csv_dao.write_records([quote_info])
        return quote_info

    async def fetch_dividend_info(self, symbol: Symbol, csv_dao: CSVGenericDAO[DividendInfo], from_: str = 'eastmoney') -> List[DividendInfo]:
        if from_ == 'eastmoney':
            return await self._fetch_dividend_info_em(symbol, csv_dao)
        elif from_ == 'sina':
            raise NotImplementedError("Sina dividend info fetching is not implemented yet.")
        else:
            raise ValueError(f"Unsupported source: {from_}. Supported sources are 'eastmoney' and 'sina'.")

    async def _fetch_dividend_info_em(self, symbol: Symbol, csv_dao: CSVGenericDAO[DividendInfo]) -> List[DividendInfo]:
        """
        从东方财富获取股票除权除息分红配股信息
        
        API接口示例：
        https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=PLAN_NOTICE_DATE&sortTypes=-1&pageSize=50&pageNumber=1&reportName=RPT_SHAREBONUS_DET&columns=ALL&quoteColumns=&source=WEB&client=WEB
        
        Returns:
            除权除息分红配股信息列表
        """
        all_dividends: List[DividendInfo] = []
        page_size = 100
        page = 1
        
        while True:
            @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
            async def _fetch_dividend_info():
                params = {
                    "sortColumns": "PLAN_NOTICE_DATE",
                    "sortTypes": "-1",
                    "pageSize": str(page_size),
                    "pageNumber": str(page),
                    "reportName": "RPT_SHAREBONUS_DET",
                    "columns": "ALL",
                    "quoteColumns": "",
                    "source": "WEB",
                    "client": "WEB",
                    "filter": f'(SECURITY_CODE="{symbol.code}")' if symbol else '',
                }
                
                url = f"https://datacenter-web.eastmoney.com/api/data/v1/get?{urlencode(params)}"
                async with self.rate_limiter_mgr.get_rate_limiter("datacenter-web.eastmoney.com"):
                    response = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
                
                if not response or not response.success:
                    raise Exception(f"Failed to fetch dividend info: {response.error if response else 'No response'}")

                payload = json.loads(extract_content(response.content, "html > body > pre"))
                if not payload.get('result') or not payload['result'].get('data'):
                    return False
                
                data_list = payload['result']['data']
                if not data_list:
                    return False
                
                page_dividends: List[DividendInfo] = []
                for item in data_list:
                    dividend_info = DividendInfo(
                        symbol=Symbol.from_string(item.get('SECUCODE', '')),
                        name=item.get('SECURITY_NAME_ABBR', ''),
                        eps=float(item.get('BASIC_EPS') or 0),
                        bvps=float(item.get('BVPS') or 0),
                        per_capital_reserve=float(item.get('PER_CAPITAL_RESERVE') or 0),
                        per_unassign_profit=float(item.get('PER_UNASSIGN_PROFIT') or 0),
                        net_profit_yoy_growth=float(item.get('PNP_YOY_RATIO') or 0),
                        total_shares=float(item.get('TOTAL_SHARES') or 0),
                        plan_notice_date=item.get('PLAN_NOTICE_DATE', '').split(' ')[0],
                        equity_record_date=item.get('EQUITY_RECORD_DATE', '').split(' ')[0] if item.get('EQUITY_RECORD_DATE') else '',
                        ex_dividend_date=item.get('EX_DIVIDEND_DATE', '').split(' ')[0] if item.get('EX_DIVIDEND_DATE') else '',
                        progress=item.get('ASSIGN_PROGRESS', ''),
                        latest_notice_date=item.get('NOTICE_DATE', '').split(' ')[0],
                        total_transfer_ratio=float(item.get('BONUS_IT_RATIO') or 0),
                        bonus_ratio=float(item.get('BONUS_RATIO') or 0),
                        transfer_ratio=float(item.get('IT_RATIO') or 0),
                        cash_dividend_ratio=float(item.get('PRETAX_BONUS_RMB') or 0),
                        dividend_yield=float(item.get('DIVIDENT_RATIO') or 0) * 100,  # 转换为百分比
                    )
                    page_dividends.append(dividend_info)
                
                all_dividends.extend(page_dividends)
                
                # 检查是否还有更多数据
                if len(page_dividends) < page_size:
                    return False

                return True
            
            continue_fetch = await _fetch_dividend_info()
            if not continue_fetch:
                break
            page += 1
        
        logging.info(f"Fetched {len(all_dividends)} dividend info records")
        csv_dao.write_records(all_dividends)
        return all_dividends

    async def fetch_stock_company_type(self, csv_dao: CSVGenericDAO[StockInfo], from_: str = 'eastmoney') -> StockInfo:
        if from_ == 'eastmoney':
            return await self._fetch_stock_company_type_em(csv_dao)
        elif from_ == 'sina':
            raise NotImplementedError("Sina stock company type fetching is not implemented yet.")
        else:
            raise ValueError(f"Unsupported source: {from_}. Supported sources are 'eastmoney' and 'sina'.")

    @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
    async def _fetch_stock_company_type_em(self, csv_dao: CSVGenericDAO[StockInfo]) -> List[StockInfo]:
        """
        从东方财富获取股票公司类型信息
        
        Returns:
            股票公司类型信息列表
        """
        all_stocks: List[StockInfo] = []
        
        params = {
        "type": "RPT_F10_PUBLIC_COMPANYTPYE",
        "sty": "ALL",
        "source": "HSF10",
        "client": "PC",
        }
        
        url = f"https://datacenter.eastmoney.com/securities/api/data/get?{urlencode(params)}"
        async with self.rate_limiter_mgr.get_rate_limiter("datacenter.eastmoney.com"):
            response = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
        
        if not response or not response.success:
            raise Exception(f"Failed to fetch company type data: {response.error if response else 'No response'}")

        payload = json.loads(extract_content(response.content, "html > body > pre"))
        if not payload.get('result') or not payload['result'].get('data'):
            raise Exception("No company type data found")
        
        data_list = payload['result']['data']
        
        # 公司类型映射
        company_type_map = {
            "1": Industry.SECURITIES,
            "2": Industry.INSURANCE, 
            "3": Industry.BANK,
            "4": Industry.GENERAL,
        }
        
        for item in data_list:
            secucode = item.get('SECUCODE', '')
            company_type_code = item.get('COMPANY_TYPE', '4')
            company_type = company_type_map.get(company_type_code, Industry.GENERAL)

            if secucode:
                symbol = Symbol.from_string(secucode)
                stock_info = StockInfo(
                    symbol=symbol,
                    name="",  # 这里没有公司名称，需要从其他接口获取
                    industry=company_type.value,  # 使用 Industry 枚举的值
                )
                all_stocks.append(stock_info)
        
        logging.info(f"Fetched {len(all_stocks)} company type records")
        csv_dao.write_records(all_stocks)
        return all_stocks

    async def fetch_stock_list(self, market_name: str, csv_dao: CSVGenericDAO[StockInfo], from_: str = 'eastmoney') -> List[StockInfo]:
        if from_ == 'eastmoney':
            return await self._fetch_stock_list_em(market_name, csv_dao)
        elif from_ == 'sina':
            raise NotImplementedError("Sina stock list fetching is not implemented yet.")
        else:
            raise ValueError(f"Unsupported source: {from_}. Supported sources are 'eastmoney' and 'sina'.")

    async def _fetch_stock_list_em(self, market_name: str, csv_dao: CSVGenericDAO[StockInfo]) -> List[StockInfo]:
        """
        从东方财富新版 push2delay 接口获取股票列表
        
        Returns:
            股票信息列表
        """
        all_stocks: List[StockInfo] = []
        page_size = 100


        if market_name not in MARKET_STOCK_LIST_FS:
            raise Exception(f"Unsupported market name: {market_name}. Supported markets: {', '.join(MARKET_STOCK_LIST_FS.keys())}")
        
        page = 1
        while True:
            @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
            async def _fetch_stock_list():
                params = {
                    'np':    '1',
                    'fltt':  '1',
                    'invt':  '2',
                    'fs':    MARKET_STOCK_LIST_FS[market_name],
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
                if not payload['data']:
                    return False
                diff = payload['data']['diff']
                if not diff:
                    return False

                page_stocks: List[StockInfo] = []
                for rec in diff:
                    code = rec.get('f12', '')
                    name = rec.get('f14', '')
                    page_stocks.append(StockInfo(
                        symbol=Symbol(
                            code=code,
                            market=get_exchange(code),
                            type=Type.STOCK.value,
                        ),
                        name=name,
                        industry=Industry.UNKNOWN.value,
                    ))

                all_stocks.extend(page_stocks)
                if len(page_stocks) < page_size:
                    return False
                
                return True
            continue_fetch = await _fetch_stock_list()
            if not continue_fetch:
                break
            page += 1

        logging.info(f"Fetched {len(all_stocks)} stocks")
        csv_dao.write_records(all_stocks)
        return all_stocks

    async def fetch_financial_data(self, symbol: Symbol, company_type: str, csv_dao: CSVGenericDAO[FinancialData], from_: str = 'eastmoney') -> List[FinancialData]:
        if from_ == 'eastmoney':
            return await self._fetch_financial_data_em(symbol, company_type, csv_dao)
        elif from_ == 'sina':
            raise NotImplementedError("Sina financial data fetching is not implemented yet.")
        else:
            raise ValueError(f"Unsupported source: {from_}. Supported sources are 'eastmoney' and 'sina'.")

    async def _fetch_financial_data_em(self, symbol: Symbol, company_type: str, csv_dao: CSVGenericDAO[FinancialData]) -> List[FinancialData]:
        """
        从东方财富获取股票财务数据，根据公司类型调用不同的财务报表接口
        
        Args:
            symbol: 股票代码
            company_type: 公司类型 ('银行', '保险', '证券', '通用')
            csv_dao: CSV数据访问对象
            
        Returns:
            财务数据列表
        """
        all_financial_data: List[FinancialData] = []
        
        # 根据公司类型确定使用的API类型
        api_types = {
            '银行': {
                'balance': ['RPT_F10_FINANCE_BBALANCE', 'F10_FINANCE_BBALANCE'],
                'income': ['RPT_F10_FINANCE_BINCOME', 'APP_F10_BINCOME'], 
                'cashflow': ['RPT_F10_FINANCE_BCASHFLOW', 'APP_F10_BCASHFLOW']
            },
            '保险': {
                'balance': ['RPT_F10_FINANCE_IBALANCE', 'F10_FINANCE_IBALANCE'],
                'income': ['RPT_F10_FINANCE_IINCOME', 'APP_F10_IINCOME'],
                'cashflow': ['RPT_F10_FINANCE_ICASHFLOW', 'APP_F10_ICASHFLOW']
            },
            '证券': {
                'balance': ['RPT_F10_FINANCE_SBALANCE', 'F10_FINANCE_SBALANCE'],
                'income': ['RPT_F10_FINANCE_SINCOME', 'APP_F10_SINCOME'],
                'cashflow': ['RPT_F10_FINANCE_SCASHFLOW', 'APP_F10_SCASHFLOW']
            },
            '综合': {
                'balance': ['RPT_F10_FINANCE_GBALANCE', 'F10_FINANCE_GBALANCE'],
                'income': ['RPT_F10_FINANCE_GINCOME', 'APP_F10_GINCOME'],
                'cashflow': ['RPT_F10_FINANCE_GCASHFLOW', 'APP_F10_GCASHFLOW']
            }
        }
        
        if company_type not in api_types:
            raise ValueError(f"Unsupported company type: {company_type}. Supported types: {list(api_types.keys())}")
        
        apis = api_types[company_type]
        page_size = 100
        
        # 获取资产负债表数据
        @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
        async def _fetch_balance_sheet():
            params = {
                "type": apis['balance'][0],
                "sty": apis['balance'][1],
                "filter": f'(SECUCODE="{symbol.code}.{symbol.market}")',
                "p": "1",
                "ps": str(page_size),
                "sr": "-1",
                "st": "REPORT_DATE",
                "source": "HSF10",
                "client": "PC"
            }
            
            url = f"https://datacenter.eastmoney.com/securities/api/data/get?{urlencode(params)}"
            async with self.rate_limiter_mgr.get_rate_limiter("datacenter.eastmoney.com"):
                response = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
            
            if not response or not response.success:
                raise Exception(f"Failed to fetch balance sheet: {response.error if response else 'No response'}")
            
            payload = json.loads(extract_content(response.content, "html > body > pre"))
            return payload.get('result', {}).get('data', [])
        
        # 获取利润表数据
        @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
        async def _fetch_income_statement():
            params = {
                "type": apis['income'][0],
                "sty": apis['income'][1],
                "filter": f'(SECUCODE="{symbol.code}.{symbol.market}")',
                "p": "1", 
                "ps": str(page_size),
                "sr": "-1",
                "st": "REPORT_DATE",
                "source": "HSF10",
                "client": "PC"
            }
            
            url = f"https://datacenter.eastmoney.com/securities/api/data/get?{urlencode(params)}"
            async with self.rate_limiter_mgr.get_rate_limiter("datacenter.eastmoney.com"):
                response = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
            
            if not response or not response.success:
                raise Exception(f"Failed to fetch income statement: {response.error if response else 'No response'}")
            
            payload = json.loads(extract_content(response.content, "html > body > pre"))
            return payload.get('result', {}).get('data', [])
        
        # 获取现金流量表数据
        @async_retry(max_retries=5, delay=1, ignore_exceptions=False)
        async def _fetch_cashflow_statement():
            params = {
                "type": apis['cashflow'][0],
                "sty": apis['cashflow'][1],
                "filter": f'(SECUCODE="{symbol.code}.{symbol.market}")',
                "p": "1",
                "ps": str(page_size), 
                "sr": "-1",
                "st": "REPORT_DATE",
                "source": "HSF10",
                "client": "PC"
            }
            
            url = f"https://datacenter.eastmoney.com/securities/api/data/get?{urlencode(params)}"
            async with self.rate_limiter_mgr.get_rate_limiter("datacenter.eastmoney.com"):
                response = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
            
            if not response or not response.success:
                raise Exception(f"Failed to fetch cashflow statement: {response.error if response else 'No response'}")
            
            payload = json.loads(extract_content(response.content, "html > body > pre"))
            return payload.get('result', {}).get('data', [])
        
        # 并发获取三个报表数据
        balance_data, income_data, cashflow_data = await asyncio.gather(
            _fetch_balance_sheet(),
            _fetch_income_statement(), 
            _fetch_cashflow_statement(),
            return_exceptions=True
        )
        
        # 处理异常情况
        if isinstance(balance_data, Exception):
            logging.warning(f"Failed to fetch balance sheet for {symbol}: {balance_data}")
            balance_data = []
        if isinstance(income_data, Exception):
            logging.warning(f"Failed to fetch income statement for {symbol}: {income_data}")
            income_data = []
        if isinstance(cashflow_data, Exception):
            logging.warning(f"Failed to fetch cashflow statement for {symbol}: {cashflow_data}")
            cashflow_data = []
        
        # 按报告日期合并数据
        merged_data = {}
        
        # 处理资产负债表数据
        for item in balance_data:
            report_date = item.get('REPORT_DATE', '').split(' ')[0]
            if report_date and report_date not in merged_data:
                merged_data[report_date] = {}
            merged_data[report_date]['balance'] = item
        
        # 处理利润表数据
        for item in income_data:
            report_date = item.get('REPORT_DATE', '').split(' ')[0]
            if report_date and report_date not in merged_data:
                merged_data[report_date] = {}
            merged_data[report_date]['income'] = item
        
        # 处理现金流量表数据
        for item in cashflow_data:
            report_date = item.get('REPORT_DATE', '').split(' ')[0]
            if report_date and report_date not in merged_data:
                merged_data[report_date] = {}
            merged_data[report_date]['cashflow'] = item
        
        # 生成FinancialData对象
        for report_date, data in merged_data.items():
            balance = data.get('balance', {})
            income = data.get('income', {})
            cashflow = data.get('cashflow', {})
            
            # 统一字段映射，处理不同公司类型的字段差异
            def safe_get_float(data_dict, key, default=0.0):
                value = data_dict.get(key)
                if value is None:
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            # 提取关键财务数据
            parent_net_profit = safe_get_float(income, 'PARENT_NETPROFIT') or safe_get_float(income, 'NETPROFIT')
            total_parent_equity = safe_get_float(balance, 'TOTAL_PARENT_EQUITY')
            total_operate_income = safe_get_float(income, 'TOTAL_OPERATE_INCOME') or safe_get_float(income, 'OPERATE_INCOME')
            total_operate_cost = safe_get_float(income, 'TOTAL_OPERATE_COST') or safe_get_float(income, 'OPERATE_COST')
            operate_expense = safe_get_float(income, 'OPERATE_EXPENSE') # 银行/保险的营业支出

            financial_data = FinancialData(
                symbol=symbol,
                report_date=report_date,
                
                # ========== 资产负债表 - 通用字段 ==========
                total_assets=safe_get_float(balance, 'TOTAL_ASSETS'),
                current_assets=safe_get_float(balance, 'TOTAL_CURRENT_ASSETS'),
                non_current_assets=safe_get_float(balance, 'TOTAL_NONCURRENT_ASSETS'),
                total_liabilities=safe_get_float(balance, 'TOTAL_LIABILITIES'),
                current_liabilities=safe_get_float(balance, 'TOTAL_CURRENT_LIAB'),
                non_current_liabilities=safe_get_float(balance, 'TOTAL_NONCURRENT_LIAB'),
                total_equity=safe_get_float(balance, 'TOTAL_EQUITY'),
                total_parent_equity=total_parent_equity,
                fixed_asset=safe_get_float(balance, 'FIXED_ASSET'),
                goodwill=safe_get_float(balance, 'GOODWILL'),
                intangible_asset=safe_get_float(balance, 'INTANGIBLE_ASSET'),
                defer_tax_asset=safe_get_float(balance, 'DEFER_TAX_ASSET'),
                defer_tax_liab=safe_get_float(balance, 'DEFER_TAX_LIAB'),
                
                # ========== 资产负债表 - 银行业特有字段 ==========
                cash_deposit_pbc=safe_get_float(balance, 'CASH_DEPOSIT_PBC'),
                loan_advance=safe_get_float(balance, 'LOAN_ADVANCE'),
                accept_deposit=safe_get_float(balance, 'ACCEPT_DEPOSIT'),
                bond_payable=safe_get_float(balance, 'BOND_PAYABLE'),
                general_risk_reserve=safe_get_float(balance, 'GENERAL_RISK_RESERVE'),
                
                # ========== 资产负债表 - 保险业特有字段 ==========
                fvtpl_finasset=safe_get_float(balance, 'FVTPL_FINASSET'),
                creditor_invest=safe_get_float(balance, 'CREDITOR_INVEST'),
                other_creditor_invest=safe_get_float(balance, 'OTHER_CREDITOR_INVEST'),
                other_equity_invest=safe_get_float(balance, 'OTHER_EQUITY_INVEST'),
                agent_trade_security=safe_get_float(balance, 'AGENT_TRADE_SECURITY'),
                
                # ========== 资产负债表 - 证券业特有字段 ==========
                customer_deposit=safe_get_float(balance, 'CUSTOMER_DEPOSIT'),
                settle_excess_reserve=safe_get_float(balance, 'SETTLE_EXCESS_RESERVE'),
                buy_resale_finasset=safe_get_float(balance, 'BUY_RESALE_FINASSET'),
                sell_repo_finasset=safe_get_float(balance, 'SELL_REPO_FINASSET'),
                trade_finasset_notfvtpl=safe_get_float(balance, 'TRADE_FINASSET_NOTFVTPL'),
                derive_finasset=safe_get_float(balance, 'DERIVE_FINASSET'),
                
                # ========== 资产负债表 - 制造业/通用行业字段 ==========
                inventory=safe_get_float(balance, 'INVENTORY'),
                accounts_receivable=safe_get_float(balance, 'ACCOUNTS_RECE'),
                note_accounts_rece=safe_get_float(balance, 'NOTE_ACCOUNTS_RECE'),
                accounts_payable=safe_get_float(balance, 'ACCOUNTS_PAYABLE'),
                note_accounts_payable=safe_get_float(balance, 'NOTE_ACCOUNTS_PAYABLE'),
                short_loan=safe_get_float(balance, 'SHORT_LOAN'),
                prepayment=safe_get_float(balance, 'PREPAYMENT'),
                
                # ========== 利润表 - 通用字段 ==========
                total_revenue=total_operate_income,
                operating_cost=total_operate_cost,
                gross_profit=total_operate_income - (total_operate_cost or operate_expense),
                operating_profit=safe_get_float(income, 'OPERATE_PROFIT'),
                total_profit=safe_get_float(income, 'TOTAL_PROFIT'),
                net_profit=parent_net_profit,
                deduct_parent_netprofit=safe_get_float(income, 'DEDUCT_PARENT_NETPROFIT'),
                basic_eps=safe_get_float(income, 'BASIC_EPS'),
                diluted_eps=safe_get_float(income, 'DILUTED_EPS'),
                roe=parent_net_profit / total_parent_equity * 100 if total_parent_equity != 0 else 0.0,
                operate_tax_add=safe_get_float(income, 'OPERATE_TAX_ADD'),
                manage_expense=safe_get_float(income, 'MANAGE_EXPENSE') or safe_get_float(income, 'BUSINESS_MANAGE_EXPENSE'),
                other_compre_income=safe_get_float(income, 'PARENT_OCI'),

                # ========== 利润表 - 银行业特有字段 ==========
                interest_net_income=safe_get_float(income, 'INTEREST_NI'),
                interest_income=safe_get_float(income, 'INTEREST_INCOME'),
                interest_expense=safe_get_float(income, 'INTEREST_EXPENSE'),
                fee_commission_net_income=safe_get_float(income, 'FEE_COMMISSION_NI'),
                credit_impairment_loss=safe_get_float(income, 'CREDIT_IMPAIRMENT_LOSS'),
                
                # ========== 利润表 - 保险业特有字段 ==========
                earned_premium=safe_get_float(income, 'EARNED_PREMIUM'),
                insurance_income=safe_get_float(income, 'INSURANCE_INCOME'),
                bank_interest_ni=safe_get_float(income, 'BANK_INTEREST_NI'),
                uninsurance_cni=safe_get_float(income, 'UNINSURANCE_CNI'),
                invest_income=safe_get_float(income, 'INVEST_INCOME'),
                fairvalue_change=safe_get_float(income, 'FAIRVALUE_CHANGE') or safe_get_float(income, 'FAIRVALUE_CHANGE_INCOME'),
                
                # ========== 利润表 - 证券业特有字段 ==========
                agent_security_ni=safe_get_float(income, 'AGENT_SECURITY_NI'),
                security_underwrite_ni=safe_get_float(income, 'SECURITY_UNDERWRITE_NI'),
                asset_manage_ni=safe_get_float(income, 'ASSET_MANAGE_NI'),
                
                # ========== 利润表 - 制造业/通用行业字段 ==========
                sale_expense=safe_get_float(income, 'SALE_EXPENSE'),
                research_expense=safe_get_float(income, 'RESEARCH_EXPENSE'),
                finance_expense=safe_get_float(income, 'FINANCE_EXPENSE'),
                asset_impairment_income=safe_get_float(income, 'ASSET_IMPAIRMENT_INCOME') or safe_get_float(income, 'ASSET_IMPAIRMENT_LOSS'),
                other_income=safe_get_float(income, 'OTHER_INCOME'),
                
                # ========== 现金流量表 - 通用字段 ==========
                net_operate_cashflow=safe_get_float(cashflow, 'NETCASH_OPERATE'),
                net_invest_cashflow=safe_get_float(cashflow, 'NETCASH_INVEST'),
                net_finance_cashflow=safe_get_float(cashflow, 'NETCASH_FINANCE'),
                total_operate_inflow=safe_get_float(cashflow, 'TOTAL_OPERATE_INFLOW'),
                total_operate_outflow=safe_get_float(cashflow, 'TOTAL_OPERATE_OUTFLOW'),
                total_invest_inflow=safe_get_float(cashflow, 'TOTAL_INVEST_INFLOW'),
                total_invest_outflow=safe_get_float(cashflow, 'TOTAL_INVEST_OUTFLOW'),
                end_cce=safe_get_float(cashflow, 'END_CCE'),

                # ========== 现金流量表 - 银行业特有字段 ==========
                deposit_iofi_other=safe_get_float(cashflow, 'DEPOSIT_IOFI_OTHER'),
                loan_advance_add=safe_get_float(cashflow, 'LOAN_ADVANCE_ADD'),
                borrow_repo_add=safe_get_float(cashflow, 'BORROW_REPO_ADD'),
                
                # ========== 现金流量表 - 保险业特有字段 ==========
                deposit_interbank_add=safe_get_float(cashflow, 'DEPOSIT_INTERBANK_ADD'),
                receive_origic_premium=safe_get_float(cashflow, 'RECEIVE_ORIGIC_PREMIUM'),
                pay_origic_compensate=safe_get_float(cashflow, 'PAY_ORIGIC_COMPENSATE'),
                
                # ========== 现金流量表 - 证券业特有字段 ==========
                disposal_tfa_add=safe_get_float(cashflow, 'DISPOSAL_TFA_ADD'),
                receive_interest_commission=safe_get_float(cashflow, 'RECEIVE_INTEREST_COMMISSION'),
                repo_business_add=safe_get_float(cashflow, 'REPO_BUSINESS_ADD'),
                pay_agent_trade=safe_get_float(cashflow, 'PAY_AGENT_TRADE'),
                
                # ========== 现金流量表 - 制造业/通用行业字段 ==========
                sales_services=safe_get_float(cashflow, 'SALES_SERVICES'),
                buy_services=safe_get_float(cashflow, 'BUY_SERVICES'),
                construct_long_asset=safe_get_float(cashflow, 'CONSTRUCT_LONG_ASSET'),
                pay_staff_cash=safe_get_float(cashflow, 'PAY_STAFF_CASH'),
                pay_all_tax=safe_get_float(cashflow, 'PAY_ALL_TAX'),
            )
            
            all_financial_data.append(financial_data)
        
        # 按报告日期排序
        all_financial_data.sort(key=lambda x: x.report_date, reverse=True)
        
        logging.info(f"Fetched {len(all_financial_data)} financial data records for {symbol} ({company_type})")
        
        csv_dao.write_records(all_financial_data)
        return all_financial_data

    def to_dict(self, data_objects: List[Any]) -> List[Dict]:
        """将数据对象转换为字典格式，便于持久化存储"""
        return [asdict(obj) for obj in data_objects]
