
# 重要指数配置
from .models import Symbol, MarketType, Type

INDEXES = [
    Symbol(code='000001', market=MarketType.SH.value, type=Type.INDEX.value), # 上证指数
    Symbol(code='399001', market=MarketType.SZ.value, type=Type.INDEX.value), # 深证成指
    Symbol(code='399006', market=MarketType.SZ.value, type=Type.INDEX.value), # 创业板指
    Symbol(code='000300', market=MarketType.SH.value, type=Type.INDEX.value), # 沪深300
    Symbol(code='000905', market=MarketType.SH.value, type=Type.INDEX.value), # 中证500
    Symbol(code='000016', market=MarketType.SH.value, type=Type.INDEX.value), # 上证50
    Symbol(code='000852', market=MarketType.SH.value, type=Type.INDEX.value), # 中证1000
    Symbol(code='399005', market=MarketType.SZ.value, type=Type.INDEX.value), # 中小板指
    Symbol(code='000688', market=MarketType.SH.value, type=Type.INDEX.value), # 科创50
    Symbol(code='000903', market=MarketType.SH.value, type=Type.INDEX.value), # 中证100
]

if __name__ == '__main__':
    symbols = []
    for index in INDEXES:
        symbols.append(index.to_string())
    print(','.join(symbols))