from typing import List, Dict, Tuple
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
from threading import Lock

from .quote_realtime import StockPrice
from fdata.dao.csv_dao import CSVStreamDAO
from fdata.spider.data_processor import DataProcessor
from fdata.spider.spider_core import AntiDetectionSpider
from fdata.utils.retry import retry

class SinaMarketProvider:
    """
    新浪财经实时行情实现
    """
    VIP_STOCK_NUM_BY_NODE_URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount?node={node}"
    VIP_STOCK_DETAIL_URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num={num}&sort=symbol&asc=1&node={node}&symbol=&_s_r_a=init"

    def __init__(self, date: str, nodes: List[str], thread_count: int = 10):
        self.date = date
        self.nodes = nodes
        self.thread_count = thread_count
        self.csv_stream_dao = CSVStreamDAO(f"realtime_sina_{date}.csv", mode='w+')
        self.csv_stream_dao.write_row(['stock_code', 'name', 'current_price', 'open_price', 'high_price', 'low_price', 'volume', 'amount', 'time'])

    def start(self, timeout: int):
        start_ts = int(time.time())

        with AntiDetectionSpider() as spider:
            node_stock_num_map = self._fetch_stock_num(spider)

        stock_data_status: Dict[str, List[StockPrice]] = {} # 存储股票数据状态

        while True:
            now = int(time.time())
            if now - start_ts > timeout:
                break

            stock_data = self._fetch_stock_data(node_stock_num_map)
            for stock in stock_data:
                # 不允许逆序或者重复数据
                l = stock_data_status.get(stock.stock_code, [])
                if len(l) > 0  and l[-1].time >= stock.time:
                    continue
                stock_data_status.setdefault(stock.stock_code, []).append(stock)
                self.csv_stream_dao.write_row([stock.stock_code, stock.name, stock.current_price, stock.open_price, stock.high_price, stock.low_price, stock.volume, stock.amount, stock.time])

            if int(time.time()) - now < 30:
                time.sleep(30 - (int(time.time()) - now))  # 确保每次获取数据间隔至少30秒

    def stop(self):
        self.csv_stream_dao.close()

    @retry(max_retries=3, delay=1)
    def _fetch_stock_num_by_node(self, spider: AntiDetectionSpider, node: str) -> int:
        url = SinaMarketProvider.VIP_STOCK_NUM_BY_NODE_URL.format(node=node)
        response = spider.crawl_url(url, filter_func=lambda x: x.url == url)
        if "data_processor" not in response:
            raise Exception(f"data processor not found in response, node: {node}")
        data_processor: DataProcessor = response["data_processor"]
        if len(data_processor.responses) == 0:
            raise Exception(f"No data found for node: {node}")
        num = bytes.fromhex(data_processor.responses[0].body)
        return int(num.decode('utf-8').strip('"'))

    def _fetch_stock_num(self, spider: AntiDetectionSpider) -> Dict[str, int]:
        m = {}
        for node in self.nodes:
            m[node] = self._fetch_stock_num_by_node(spider, node)
        return m

    @retry(max_retries=3, delay=1)
    def _fetch_stock_data_by_page(self, spider: AntiDetectionSpider, node: str, page: int, num: int) -> List[StockPrice]:
        url = SinaMarketProvider.VIP_STOCK_DETAIL_URL.format(page=page, num=num, node=node)
        response = spider.crawl_url(url, filter_func=lambda x: x.url == url)
        if "data_processor" not in response:
            raise Exception(f"data processor not found in response, node: {node}")
        data_processor: DataProcessor = response["data_processor"]
        if len(data_processor.responses) == 0:
            raise Exception(f"No data found for node: {node}")

        data = bytes.fromhex(data_processor.responses[0].body).decode('utf-8')
        data = json.loads(data)

        stock_data = []
        for item in data:
            stock_data.append(StockPrice(
                stock_code=item['symbol'],
                name=item['name'],
                current_price=float(item['trade']),
                open_price=float(item['open']),
                high_price=float(item['high']),
                low_price=float(item['low']),
                volume=float(item['volume']) if 'volume' in item else 0.0,
                amount=float(item['amount']) if 'amount' in item else 0.0,
                time=int(datetime.strptime(self.date + ' ' + item['ticktime'], "%Y-%m-%d %H:%M:%S").timestamp())
            ))
        return stock_data

    def _fetch_stock_data(self, node_stock_num_map: Dict[str, int]) -> List[StockPrice]: 
        try:
            with AntiDetectionSpider() as spider:
                with ThreadPoolExecutor(max_workers=self.thread_count) as executor:
                    futures = []
                    for node, num in node_stock_num_map.items():
                        for index in range((num + 99) // 100):
                            futures.append(executor.submit(self._fetch_stock_data_by_page, spider, node, index + 1, 100))

                stock_data = []
                for future in as_completed(futures):
                    stock_data = future.result()
                    stock_data.extend(stock_data)
            return stock_data
        except Exception as e:
            print(f"Error fetching stock data: {e}")
            return []

if __name__ == '__main__':
    provider = SinaMarketProvider(date='2023-10-01', nodes=['hs_a'], thread_count=20)
    provider.start(60)
    provider.stop()
