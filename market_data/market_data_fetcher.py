import json
import time
from typing import Dict, List, Any
from dataclasses import asdict
from urllib.parse import urlencode
import logging
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

    @async_retry(max_retries=5, delay=1, ignore_exceptions=True)
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

    @async_retry(max_retries=5, delay=1, ignore_exceptions=True)
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

    @async_retry(max_retries=5, delay=1, ignore_exceptions=True)
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
            @async_retry(max_retries=5, delay=1, ignore_exceptions=True)
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
            @async_retry(max_retries=5, delay=1, ignore_exceptions=True)
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

    async def fetch_financial_data(self, symbol: Symbol, csv_dao: CSVGenericDAO[FinancialData], from_: str = 'eastmoney') -> List[FinancialData]:
        if from_ == 'eastmoney':
            return await self._fetch_financial_data_em(symbol, csv_dao)
        elif from_ == 'sina':
            raise NotImplementedError("Sina financial data fetching is not implemented yet.")
        else:
            raise ValueError(f"Unsupported source: {from_}. Supported sources are 'eastmoney' and 'sina'.")

    async def _fetch_financial_data_em(self, symbol: Symbol, csv_dao: CSVGenericDAO[FinancialData]) -> List[FinancialData]:
        """
        从东方财富新版接口获取财务三表（利润表、资产负债表、现金流量表）并按 REPORT_DATE 合并
        
        API接口示例：
        - 资产负债表: https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_F10_FINANCE_BBALANCE&sty=F10_FINANCE_BBALANCE&filter=(SECUCODE="600000.SH")&p=1&ps=5&sr=-1&st=REPORT_DATE&source=HSF10&client=PC
        - 利润表: https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_F10_FINANCE_BINCOME&sty=APP_F10_BINCOME&filter=(SECUCODE="600000.SH")&p=1&ps=5&sr=-1&st=REPORT_DATE&source=HSF10&client=PC
        - 现金流量表: https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_F10_FINANCE_BCASHFLOW&sty=APP_F10_BCASHFLOW&filter=(SECUCODE="600000.SH")&p=1&ps=5&sr=-1&st=REPORT_DATE&source=HSF10&client=PC
        """
        
        tables = [
            ("RPT_F10_FINANCE_BBALANCE", "F10_FINANCE_BBALANCE"),   # 资产负债表
            ("RPT_F10_FINANCE_BINCOME", "APP_F10_BINCOME"),         # 利润表
            ("RPT_F10_FINANCE_BCASHFLOW", "APP_F10_BCASHFLOW"),     # 现金流量表
        ]
        records: Dict[str, Dict[str, Any]] = {}
        page_size = 50
        
        for table_type, sty in tables:
            page = 1
            while True:
                @async_retry(max_retries=5, delay=1, ignore_exceptions=True)
                async def _fetch_financial_data():
                    params = {
                        "type": table_type,
                        "sty": sty,
                        "filter": f'(SECUCODE="{symbol.code}.{symbol.market}")',
                        "p": page,
                        "ps": page_size,
                        "sr": -1,
                        "st": "REPORT_DATE",
                        "source": "HSF10",
                        "client": "PC",
                    }
                    url = f"https://datacenter.eastmoney.com/securities/api/data/get?{urlencode(params)}"
                    async with self.rate_limiter_mgr.get_rate_limiter("datacenter.eastmoney.com"):
                        resp = await self.spider.crawl_url(url, headers=self.eastmoney_headers)
                    if not resp or not resp.success:
                        raise Exception(f"Failed to fetch {table_type} for {symbol}: {resp.error if resp else 'No response'}")

                    payload = json.loads(extract_content(resp.content, "html > body > pre"))
                    if not payload.get('result') or not payload['result'].get('data'):
                        return False
                    
                    rows = payload['result']['data']
                    if not rows:
                        return False
                    
                    for item in rows:
                        report_date = item.get("REPORT_DATE", "").split(" ")[0]
                        if not report_date:
                            continue
                            
                        record = records.setdefault(report_date, {"symbol": symbol, "report_date": report_date})
                        
                        if table_type == "RPT_F10_FINANCE_BBALANCE":  # 资产负债表
                            record["total_assets"] = float(item.get("TOTAL_ASSETS") or 0)
                            record["current_assets"] = float(item.get("TOTAL_CURRENT_ASSETS") or 0)
                            record["non_current_assets"] = float(item.get("TOTAL_NONCURRENT_ASSETS") or 0)
                            record["total_liabilities"] = float(item.get("TOTAL_LIABILITIES") or 0)
                            record["current_liabilities"] = float(item.get("TOTAL_CURRENT_LIAB") or 0)
                            record["non_current_liabilities"] = float(item.get("TOTAL_NONCURRENT_LIAB") or 0)
                            record["total_equity"] = float(item.get("TOTAL_EQUITY") or 0)
                        elif table_type == "RPT_F10_FINANCE_BINCOME":  # 利润表
                            record["total_revenue"] = float(item.get("OPERATE_INCOME") or 0)
                            record["operating_cost"] = float(item.get("OPERATE_EXPENSE") or 0)
                            record["operating_profit"] = float(item.get("OPERATE_PROFIT") or 0)
                            record["profit_before_tax"] = float(item.get("TOTAL_PROFIT") or 0)
                            record["net_profit"] = float(item.get("PARENT_NETPROFIT") or 0)
                            record["eps"] = float(item.get("BASIC_EPS") or 0)
                            # 毛利 = 营业收入 - 营业成本
                            record["gross_profit"] = record["total_revenue"] - record["operating_cost"]
                        elif table_type == "RPT_F10_FINANCE_BCASHFLOW":  # 现金流量表
                            record["net_operate_cashflow"] = float(item.get("NETCASH_OPERATE") or 0)
                            record["net_invest_cashflow"] = float(item.get("NETCASH_INVEST") or 0)
                            record["net_finance_cashflow"] = float(item.get("NETCASH_FINANCE") or 0)
                            # 自由现金流 = 经营活动现金流 - 构建长期资产支出
                            record["free_cashflow"] = record["net_operate_cashflow"] - float(item.get('CONSTRUCT_LONG_ASSET') or 0)
                    
                    if len(rows) < page_size:
                        return False
                    
                    return True

                continue_fetch = await _fetch_financial_data()
                if not continue_fetch:
                    break
                page += 1
        
        # 构建 FinancialData 列表
        result: List[FinancialData] = []
        for report_date, record in sorted(records.items(), reverse=True):
            # 计算 ROE (净资产收益率) = 归属母公司净利润 / 归属母公司股东权益 * 100
            roe = 0.0
            if record.get("total_equity", 0.0) > 0:
                roe = (record.get("net_profit", 0.0) / record.get("total_equity", 1.0)) * 100
            
            result.append(
                FinancialData(
                    symbol=record["symbol"],
                    report_date=record["report_date"],
                    total_revenue=record.get("total_revenue", 0.0),
                    operating_cost=record.get("operating_cost", 0.0),
                    gross_profit=record.get("gross_profit", 0.0),
                    operating_profit=record.get("operating_profit", 0.0),
                    profit_before_tax=record.get("profit_before_tax", 0.0),
                    net_profit=record.get("net_profit", 0.0),
                    eps=record.get("eps", 0.0),
                    roe=roe,
                    total_assets=record.get("total_assets", 0.0),
                    current_assets=record.get("current_assets", 0.0),
                    non_current_assets=record.get("non_current_assets", 0.0),
                    total_liabilities=record.get("total_liabilities", 0.0),
                    current_liabilities=record.get("current_liabilities", 0.0),
                    non_current_liabilities=record.get("non_current_liabilities", 0.0),
                    total_equity=record.get("total_equity", 0.0),
                    net_operate_cashflow=record.get("net_operate_cashflow", 0.0),
                    net_invest_cashflow=record.get("net_invest_cashflow", 0.0),
                    net_finance_cashflow=record.get("net_finance_cashflow", 0.0),
                    free_cashflow=record.get("free_cashflow", 0.0),
                )
            )
        
        logging.info(f"Fetched {len(result)} financial data records for {symbol}")
        csv_dao.write_records(result)
        return result

    def to_dict(self, data_objects: List[Any]) -> List[Dict]:
        """将数据对象转换为字典格式，便于持久化存储"""
        return [asdict(obj) for obj in data_objects]
