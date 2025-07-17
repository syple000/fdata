
# 重要指数配置
from .models import Symbol, MarketType, Type

INDEXES = [
    Symbol(code='000001', name='上证指数', market=MarketType.SH.value, type=Type.INDEX.value),
    Symbol(code='399001', name='深证成指', market=MarketType.SZ.value, type=Type.INDEX.value),
    Symbol(code='399006', name='创业板指', market=MarketType.SZ.value, type=Type.INDEX.value),
    Symbol(code='000300', name='沪深300', market=MarketType.SH.value, type=Type.INDEX.value),
    Symbol(code='000905', name='中证500', market= MarketType.SH.value, type=Type.INDEX.value),
    Symbol(code='000016', name='上证50', market=MarketType.SH.value, type=Type.INDEX.value),
    Symbol(code='000852', name='中证1000', market=MarketType.SH.value, type=Type.INDEX.value),
    Symbol(code='399005', name='中小板指', market=MarketType.SZ.value, type=Type.INDEX.value),
    Symbol(code='000688', name='科创50', market=MarketType.SH.value, type=Type.INDEX.value),
    Symbol(code='000903', name='中证100', market= MarketType.SH.value, type=Type.INDEX.value),
]