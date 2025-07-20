
from .models import Industry
# 指数代码映射市场过滤条件，stock_list获取时使用

MARKET_STOCK_LIST_FS = {
    '上证指数': 'm:1+t:2,m:1+t:23',
    '深证成指': 'm:0+t:6,m:0+t:80',
    '北交所': 'm:0+t:81+s:2048',
    '创业板指': 'm:0+t:80',
    '沪深300': 'b:bk0500+f:!50',
    '上证50': 'b:bk0611+f:!50',
    '科创50': 'm:1+t:23',
}

# 公司类型映射财报枚举
COMPANY_TYPE_MAP = {
    Industry.BANK.value: 'B',        # 银行
    Industry.INSURANCE.value: 'I',   # 保险
    Industry.SECURITIES.value: 'S',  # 证券
    Industry.GENERAL.value: 'G',     # 综合
}