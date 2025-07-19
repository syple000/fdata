from enum import Enum
from dataclasses import dataclass

class KLineType(Enum):
    """K线类型"""
    DAILY = '101'       # 日线
    WEEKLY = '102'      # 周线
    MONTHLY = '103'     # 月线
    MIN5 = '5'          # 5分钟线
    MIN15 = '15'        # 15分钟线
    MIN30 = '30'        # 30分钟线
    MIN60 = '60'        # 60分钟线

class AdjustType(Enum):
    """复权类型"""
    NONE = '0'          # 不复权
    FORWARD = '1'       # 前复权
    BACKWARD = '2'      # 后复权

class MarketType(Enum):
    SH = 'SH'  # 上海证券交易所
    SZ = 'SZ'  # 深圳证券交易所
    BJ = 'BJ'  # 北京证券交易所

class Type(Enum):
    STOCK = 'STOCK'  # 股票
    FUND = 'FUND'    # 基金
    BOND = 'BOND'    # 债券
    INDEX = 'INDEX'  # 指数
    ETF = 'ETF'      # 交易型基金

@dataclass
class Symbol:
    code: str # 编码
    market: str # 市场类型：SH/SZ/BJ等
    type: str # 类型：股票/基金/债券等

    def to_string(self) -> str:
        """将Symbol对象转换为字符串格式"""
        return f"{self.code}.{self.market}.{self.type}" if self.type != Type.STOCK.value else f"{self.code}.{self.market}"

    @staticmethod
    def from_string(symbol_str: str) -> 'Symbol':
        """从字符串创建Symbol对象，格式为 'code.market[.type]'"""
        parts = symbol_str.split('.')
        if len(parts) < 2:
            raise ValueError(f"Invalid symbol format: {symbol_str}.")
        if len(parts) == 2:
            return Symbol(code=parts[0], market=parts[1], type=Type.STOCK.value)
        else:
            return Symbol(code=parts[0], market=parts[1], type=parts[2])
    
    def __eq__(self, other):
        """定义相等操作，基于code和market进行比较"""
        if not isinstance(other, Symbol):
            return False
        return self.code == other.code and self.market == other.market

    def __hash__(self):
        """定义哈希操作，使Symbol可以用作字典键或集合元素"""
        return hash((self.code, self.market))

@dataclass
class StockInfo:
    symbol: Symbol
    name: str  # 名称

@dataclass
class RealTimeQuote:
    """实时行情数据结构"""
    symbol: Symbol  # 股票代码
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
    symbol: Symbol
    date: str # 日期，格式为 'YYYY-MM-DD'，如果是分钟线，格式为 'YYYY-MM-DD HH:MM:SS'
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    turnover: float
    change_percent: float

@dataclass
class FinancialData:
    """财务数据结构，包含利润表、资产负债表和现金流量表的重要字段"""
    symbol: Symbol
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

@dataclass
class StockQuoteInfo:
    symbol: Symbol
    name: str
    
    # 价格信息
    open_price: float        # 今开
    prev_close: float        # 昨收
    high_price: float        # 最高
    low_price: float         # 最低
    limit_up: float          # 涨停
    limit_down: float        # 跌停
    
    # 交易信息
    turnover_rate: float     # 换手率 (%)
    volume_ratio: float      # 量比
    volume: int              # 成交量
    turnover: float          # 成交额
    
    # 估值信息
    pe_dynamic: float        # 市盈率(动态)
    pe_ttm: float            # 市盈率(TTM)
    pe_lyr: float            # 市盈率(静态)
    pb_ratio: float          # 市净率
    total_market_cap: float  # 总市值
    circulating_market_cap: float  # 流通市值

@dataclass
class DividendInfo:
    """分红配股数据结构"""
    symbol: Symbol                    # 股票代码
    name: str                        # 股票名称
    eps: float                       # 每股收益(元)
    bvps: float                      # 每股净资产(元)
    per_capital_reserve: float       # 每股公积金(元)
    per_unassign_profit: float       # 每股未分配利润(元)
    net_profit_yoy_growth: float     # 净利润同比增长(%)
    total_shares: float              # 总股本(亿)
    plan_notice_date: str            # 预案公告日
    equity_record_date: str          # 股权登记日
    ex_dividend_date: str            # 除权除息日
    progress: str                    # 方案进度
    latest_notice_date: str          # 最新公告日期
    total_transfer_ratio: float      # 送转总比例
    bonus_ratio: float               # 送股比例
    transfer_ratio: float            # 转股比例
    cash_dividend_ratio: float       # 现金分红比例
    dividend_yield: float            # 股息率(%)

# 传入股票code，返回对应的交易所
def get_exchange(code: str) -> str:
    if code.startswith('6'):
        return MarketType.SH.value
    elif code.startswith(('0', '3')):
        return MarketType.SZ.value
    elif code.startswith('8') or code.startswith('4') or code.startswith('920'):
        return MarketType.BJ.value
    else:
        raise ValueError(f"Unsupported stock code: {code}. Expected code starting with 0, 3, 6, 8 or 4 for SZ, SH or BJ markets respectively.")

