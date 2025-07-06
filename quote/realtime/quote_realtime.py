from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict

@dataclass
class StockPrice:
    """
    股票实时行情结构体
    """
    stock_code: str          # 股票代码
    name: str                # 股票名称
    current_price: float     # 当前价格
    open_price: float        # 开盘价
    high_price: float        # 最高价
    low_price: float         # 最低价
    volume: float            # 成交量
    amount: float            # 成交额
    time: int                # 数据时间戳


class RealTimeMarketInterface(ABC):
    """
    抽象基类：定义实时行情接口
    """
    @abstractmethod
    def get_stock_price(self, stock_code: str) -> StockPrice:
        """
        获取单只股票的实时行情
        :param stock_code: 股票代码
        :return: 实时行情数据（字典格式）
        """
        pass

    @abstractmethod
    def get_multiple_stock_prices(self, stock_codes: List[str]) -> Dict[str, StockPrice]:
        """
        获取多只股票的实时行情
        :param stock_codes: 股票代码列表
        :return: 多只股票的实时行情数据（字典格式）
        """
        pass

class MarketAPIManager:
    """
    市场行情接口管理器：支持多种数据源
    """
    def __init__(self, api: RealTimeMarketInterface):
        self.api = api

    def get_stock_price(self, stock_code: str) -> StockPrice:
        return self.api.get_stock_price(stock_code)

    def get_multiple_stock_prices(self, stock_codes: List[str]) -> Dict[str, StockPrice]:
        return self.api.get_multiple_stock_prices(stock_codes)
