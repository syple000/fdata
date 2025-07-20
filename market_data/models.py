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

class Industry(Enum):
    """行业分类"""
    BANK = '银行'
    INSURANCE = '保险' 
    SECURITIES = '证券'
    GENERAL = '综合'

    UNKNOWN = '未知'  # 未知行业

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
    industry: str  # 行业

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
    """财务数据结构，适用于银行、保险、证券、综合等不同行业"""
    symbol: Symbol
    report_date: str

    # ========== 资产负债表 - 通用字段 ==========
    total_assets: float                  # 资产总计 - 通用
    current_assets: float                # 流动资产合计 - 通用
    non_current_assets: float            # 非流动资产合计 - 通用
    total_liabilities: float             # 负债合计 - 通用
    current_liabilities: float           # 流动负债合计 - 通用
    non_current_liabilities: float       # 非流动负债合计 - 通用
    total_equity: float                  # 股东权益合计 - 通用
    fixed_asset: float                   # 固定资产净额 - 通用
    goodwill: float                      # 商誉 - 通用
    intangible_asset: float              # 无形资产 - 通用
    defer_tax_asset: float               # 递延所得税资产 - 通用
    defer_tax_liab: float                # 递延所得税负债 - 通用

    # ========== 资产负债表 - 银行业特有字段 ==========
    cash_deposit_pbc: float              # 存放央行款项（银行）
    loan_advance: float                  # 发放贷款及垫款（银行）
    accept_deposit: float                # 吸收存款（银行）
    bond_payable: float                  # 应付债券（银行/保险重要）
    general_risk_reserve: float          # 一般风险准备（银行/证券）

    # ========== 资产负债表 - 保险业特有字段 ==========
    fvtpl_finasset: float               # 以公允价值计量资产（保险）
    creditor_invest: float               # 债权投资（保险）
    other_creditor_invest: float         # 其他债权投资（保险）
    other_equity_invest: float           # 其他权益工具投资（保险）
    agent_trade_security: float          # 代理买卖证券款（保险）

    # ========== 资产负债表 - 证券业特有字段 ==========
    customer_deposit: float              # 客户资金存款（证券）
    settle_excess_reserve: float         # 结算备付金（证券）
    buy_resale_finasset: float           # 买入返售金融资产（证券）
    sell_repo_finasset: float            # 卖出回购金融资产款（证券）
    trade_finasset_notfvtpl: float       # 交易性金融资产（证券）
    derive_finasset: float               # 衍生金融资产（证券）

    # ========== 资产负债表 - 制造业/综合行业字段 ==========
    inventory: float                     # 存货（制造业）
    accounts_receivable: float           # 应收账款（制造业）
    accounts_payable: float              # 应付账款（制造业）
    short_loan: float                    # 短期借款（制造业）
    prepayment: float                    # 预付款项（制造业）

    # ========== 利润表 - 通用字段 ==========
    total_revenue: float                 # 营业收入 - 通用
    operating_cost: float                # 营业成本 - 通用  
    gross_profit: float                  # 毛利润 - 通用
    operating_profit: float              # 营业利润 - 通用
    total_profit: float                  # 利润总额 - 通用
    net_profit: float                    # 归属母公司净利润 - 通用
    basic_eps: float                     # 基本每股收益 - 通用
    roe: float                           # 净资产收益率 - 通用
    operate_tax_add: float               # 营业税金及附加 - 通用
    manage_expense: float                # 管理费用 - 通用

    # ========== 利润表 - 银行业特有字段 ==========
    interest_net_income: float           # 利息净收入（银行）
    interest_income: float               # 利息收入（银行）
    interest_expense: float              # 利息支出（银行）
    fee_commission_net_income: float     # 手续费及佣金净收入（银行/证券）
    credit_impairment_loss: float        # 信用减值损失（银行）

    # ========== 利润表 - 保险业特有字段 ==========
    earned_premium: float                # 已赚保费（保险）
    insurance_income: float              # 保险业务收入（保险）
    bank_interest_ni: float              # 银行业务利息净收入（保险）
    uninsurance_cni: float               # 非保险业务手续费净收入（保险）
    invest_income: float                 # 投资收益 - 通用但保险业重要
    fairvalue_change: float              # 公允价值变动损益 - 通用但保险业重要

    # ========== 利润表 - 证券业特有字段 ==========
    agent_security_ni: float             # 代理买卖证券净收入（证券）
    security_underwrite_ni: float        # 证券承销净收入（证券）
    asset_manage_ni: float               # 资产管理净收入（证券）

    # ========== 利润表 - 制造业/综合行业字段 ==========
    sale_expense: float                  # 销售费用（制造业）
    finance_expense: float               # 财务费用 - 通用
    asset_impairment_income: float       # 资产减值损失（制造业）
    other_income: float                  # 其他收益 - 通用

    # ========== 现金流量表 - 通用字段 ==========
    net_operate_cashflow: float          # 经营活动现金流量净额 - 通用
    net_invest_cashflow: float           # 投资活动现金流量净额 - 通用
    net_finance_cashflow: float          # 筹资活动现金流量净额 - 通用
    total_operate_inflow: float          # 经营活动现金流入小计 - 通用
    total_operate_outflow: float         # 经营活动现金流出小计 - 通用
    total_invest_inflow: float           # 投资活动现金流入小计 - 通用
    total_invest_outflow: float          # 投资活动现金流出小计 - 通用

    # ========== 现金流量表 - 银行业特有字段 ==========
    deposit_iofi_other: float            # 存放同业及其他金融机构款项净增加额（银行）
    loan_advance_add: float              # 客户贷款及垫款净增加额（银行）
    borrow_repo_add: float               # 同业及其他金融机构存放款项净增加额（银行）

    # ========== 现金流量表 - 保险业特有字段 ==========
    deposit_interbank_add: float         # 存放同业款项净增加额（保险）
    receive_origic_premium: float        # 收到原保险合同保费（保险）
    pay_origic_compensate: float         # 支付原保险合同赔付款项（保险）

    # ========== 现金流量表 - 证券业特有字段 ==========
    disposal_tfa_add: float              # 处置交易性金融资产净增加额（证券）
    receive_interest_commission: float    # 收取利息、手续费及佣金（证券）
    repo_business_add: float             # 回购业务资金净增加额（证券）
    pay_agent_trade: float               # 支付给客户及代理证券款（证券）

    # ========== 现金流量表 - 制造业/综合行业字段 ==========
    sales_services: float                # 销售商品、提供劳务收到的现金（制造业）
    buy_services: float                  # 购买商品、接受劳务支付的现金（制造业）
    construct_long_asset: float          # 购建固定资产、无形资产支付的现金 - 通用
    pay_staff_cash: float                # 支付给职工以及为职工支付的现金 - 通用
    pay_all_tax: float                   # 支付的各项税费 - 通用

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

