from decimal import Decimal
from typing import Dict, List
import pandas as pd
import os
import re
from datetime import datetime

from fdata.market_data.models import KLineType

def parse_ts(time_str: str) -> int:
    # 判断格式，支持格式1: %Y-%m-%d %H:%M:%S，格式2: %Y-%m-%d
    # 正则匹配判断格式
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", time_str):
        return int(datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").timestamp())
    elif re.match(r"^\d{4}-\d{2}-\d{2}$", time_str):
        return int(datetime.strptime(time_str, "%Y-%m-%d").timestamp())
    else:
        raise ValueError(f"Unsupported time format: {time_str}")

def forward_adjust(kline_df: pd.DataFrame, dividend_df: pd.DataFrame) -> pd.DataFrame:
    # 保护原kline df
    kline_df = kline_df.copy()

    i = 0
    while i < len(dividend_df):
        dividend_row = dividend_df.iloc[i]

        total_transfer_ratio = Decimal(str(dividend_row['total_transfer_ratio']))
        cash_dividend = Decimal(str(dividend_row['cash_dividend']))

        def recalc(x):
            x = Decimal(x)
            x = x*Decimal('10')/(Decimal('10')+total_transfer_ratio) - cash_dividend/(Decimal('10')+total_transfer_ratio)
            return str(x)

        ex_dividend_date = parse_ts(dividend_row['ex_dividend_date'])
        
        # 找到除权除息日期之前的所有数据重计算（turnover复权后无效）
        mask = kline_df['date'].apply(parse_ts) < ex_dividend_date
        kline_df.loc[mask, 'open_price'] = kline_df.loc[mask, 'open_price'].apply(recalc)
        kline_df.loc[mask, 'close_price'] = kline_df.loc[mask, 'close_price'].apply(recalc)
        kline_df.loc[mask, 'high_price'] = kline_df.loc[mask, 'high_price'].apply(recalc)
        kline_df.loc[mask, 'low_price'] = kline_df.loc[mask, 'low_price'].apply(recalc)

        i += 1
    
    return kline_df
    

class BacktestDataFeed:
    # 在回溯场景下，文件有所有信息（包括回溯时间后的数据），依赖时间游标控制可见性
    class IndexWrapper:
        def __init__(self, df: pd.DataFrame, date_column: str, interval: int):
            self.df = df
            self.date_column = date_column # 日期列名
            self.interval = interval # 间隔时间，单位秒
            self.next_index = 0 # 下一个索引位置

        def till(self, time: str) -> pd.DataFrame:
            if self.next_index >= len(self.df): # 已经是最新数据，全量返回
                return self.df

            ts = parse_ts(time)
            while self.next_index < len(self.df) and \
                ts >= parse_ts(self.df.iloc[self.next_index][self.date_column]) + self.interval:
                self.next_index += 1
            
            return self.df.iloc[:self.next_index]

    def __init__(self, archive_path: str, symbols: List[str], kline_type: KLineType = KLineType.DAILY):
        self._archive_path = archive_path
        self._symbols = symbols
        self._kline_type = kline_type

        self._symbol_data_map: Dict[str, Dict[str, BacktestDataFeed.IndexWrapper]] = {}
        for symbol in self._symbols:
            self._symbol_data_map[symbol] = self._load_symbol(symbol)

    def _load_symbol(self, symbol: str) -> Dict[str, IndexWrapper]:
        # 1. 加载除权除息数据
        dividend_info = pd.read_csv(os.path.join(self._archive_path, symbol, 'dividend_info.csv'), dtype=str)
        # 2. 加载财务数据
        financial_data = pd.read_csv(os.path.join(self._archive_path, symbol, 'financial_data.csv'), dtype=str)
        # 3. 加载k线数据
        if self._kline_type == KLineType.DAILY:
            kline_data = pd.read_csv(os.path.join(self._archive_path, symbol, f'historical_data_{KLineType.DAILY.name}_NONE.csv'), dtype=str)
        else: # 暂不支持
            raise ValueError(f"Unsupported kline type: {self._kline_type}")

        return {
            'dividend_info': self.IndexWrapper(dividend_info, 'ex_dividend_date', 0),
            'financial_data': self.IndexWrapper(financial_data, 'report_date', 24*3600),
            'kline_data': self.IndexWrapper(kline_data, 'date', 24*3600)
        }

    def get(self, date: str) -> Dict[str, pd.DataFrame]:
        result = {}
        for symbol, data_map in self._symbol_data_map.items():
            result[symbol] = {
                'dividend_info': data_map['dividend_info'].till(date),
                'financial_data': data_map['financial_data'].till(date),
                'kline_data': data_map['kline_data'].till(date)
            }
        return result

class DateRange:
    # 返回一个时间范围内的所有日期: %Y-%m-%d，yield逐个返回
    def __init__(self, start: str, end: str):
        self.start = parse_ts(start)
        self.end = parse_ts(end)
        self.current = self.start

    def __iter__(self):
        while self.current <= self.end:
            yield datetime.fromtimestamp(self.current).strftime("%Y-%m-%d")
            self.current += 24*3600  # 增加一天的秒数

if __name__ == "__main__":
    # 测试沪深300部分股票
    archive_path = 'archive'
    symbols = ['000001.SZ', '000002.SZ', '600000.SH']

    data_feed = BacktestDataFeed(archive_path, symbols)

    data = data_feed.get('2025-06-12')
    assert data['000001.SZ']['kline_data'].iloc[-1]['date'] == '2025-06-11', "Last kline date should be 2025-06-11"
    assert data['000001.SZ']['dividend_info'].iloc[-1]['ex_dividend_date'] == '2025-06-12', "Last dividend date should be 2025-06-12"
    assert data['000001.SZ']['financial_data'].iloc[-1]['report_date'] == '2025-03-31', "Last financial report date should be 2025-03-31"

    for symbol in symbols:
        assert not data[symbol]['kline_data'].empty, f"Kline data for {symbol} should not be empty"
        assert not data[symbol]['dividend_info'].empty, f"Dividend info for {symbol} should not be empty"
        assert not data[symbol]['financial_data'].empty, f"Financial data for {symbol} should not be empty"

    for date in DateRange('2025-06-01', datetime.now().strftime("%Y-%m-%d")):
        _ = data_feed.get(date)

    kline_df = pd.read_csv(os.path.join(archive_path, '000001.SZ', 'historical_data_DAILY_NONE.csv'), dtype=str)
    dividend_df = pd.read_csv(os.path.join(archive_path, '000001.SZ', 'dividend_info.csv'), dtype=str)
    adjusted_kline_df = forward_adjust(kline_df, dividend_df)
    adjusted_kline_df.to_csv(os.path.join('adjusted_historical_data_DAILY_FORWARD.csv'), index=False)
