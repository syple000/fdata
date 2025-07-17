
# 从dump的股票数据，提取每只股票的数据（实时行情、历史行情、财务）等
import pandas as pd
import argparse
import os
from datetime import datetime
import logging

def extract_single_stock_financial_data(dump_dir, dump_date, archive_dir):
    target_path = os.path.join(dump_dir, 'financial_data')
    dump_dates = os.listdir(target_path)
    if dump_date not in dump_dates:
        logging.info(f"{dump_date} not in {dump_dates}, skip")
        return
    dump_date_path = os.path.join(target_path, dump_date)
    batch_nums = os.listdir(dump_date_path) # 全市场股票，多批次dump
    for batch_num in batch_nums:
        batch_path = os.path.join(dump_date_path, batch_num)
        if not os.path.isdir(batch_path):
            continue
        stock_files = os.listdir(batch_path)
        for stock_file in stock_files:
            stock_path = os.path.join(batch_path, stock_file)
            if not os.path.isfile(stock_path):
                continue
            stock_data = pd.read_csv(stock_path, encoding='utf-8')
            grouped = stock_data.groupby('symbol')
            for symbol, group_df in grouped:
                # 创建目标目录
                output_dir = os.path.join(archive_dir, symbol, dump_date)
                os.makedirs(output_dir, exist_ok=True)
                
                # 保存到目标目录
                output_file = os.path.join(output_dir, 'financial.csv')
                group_df = group_df.sort_values(by='report_date')
                group_df.to_csv(output_file, index=False, encoding='utf-8')

def extract_single_stock_historical_data(dump_dir, dump_date, archive_dir):
    target_path = os.path.join(dump_dir, 'historical_data')
    dump_dates = os.listdir(target_path)
    if dump_date not in dump_dates:
        logging.info(f"{dump_date} not in {dump_dates}, skip")
        return
    dump_date_path = os.path.join(target_path, dump_date)
    batch_nums = os.listdir(dump_date_path)  # 全市场股票，多批次dump
    for batch_num in batch_nums:
        batch_path = os.path.join(dump_date_path, batch_num)
        if not os.path.isdir(batch_path):
            continue
        date_ranges = os.listdir(batch_path)
        for date_range in date_ranges:
            date_range_path = os.path.join(batch_path, date_range)
            if not os.path.isdir(date_range_path):
                continue
            kline_types = os.listdir(date_range_path)
            for kline_type in kline_types:
                kline_type_path = os.path.join(date_range_path, kline_type)
                if not os.path.isdir(kline_type_path):
                    continue
                adjust_types = os.listdir(kline_type_path)
                for adjust_type in adjust_types:
                    adjust_type_path = os.path.join(kline_type_path, adjust_type)
                    if not os.path.isdir(adjust_type_path):
                        continue
                    stock_files = os.listdir(adjust_type_path)
                    for stock_file in stock_files:
                        stock_path = os.path.join(adjust_type_path, stock_file)
                        if not os.path.isfile(stock_path):
                            continue
                        stock_data = pd.read_csv(stock_path, encoding='utf-8')
                        grouped = stock_data.groupby('symbol')
                        for symbol, group_df in grouped:
                            # 创建目标目录
                            output_dir = os.path.join(archive_dir, symbol, dump_date, date_range)
                            os.makedirs(output_dir, exist_ok=True)

                            # 保存到目标目录
                            output_file = os.path.join(output_dir, f'historical_data_{kline_type}_{adjust_type}.csv')
                            group_df = group_df.sort_values(by='date')
                            group_df.to_csv(output_file, index=False, encoding='utf-8')

def extract_single_stock_realtime_quotes(dump_dir, dump_date, archive_dir):
    target_path = os.path.join(dump_dir, 'realtime_quotes')
    dump_dates = os.listdir(target_path)
    if dump_date not in dump_dates:
        logging.info(f"{dump_date} not in {dump_dates}, skip")
        return
    dump_date_path = os.path.join(target_path, dump_date)
    batch_nums = os.listdir(dump_date_path)  # 全市场股票，多批次dump
    for batch_num in batch_nums:
        batch_path = os.path.join(dump_date_path, batch_num)
        if not os.path.isdir(batch_path):
            continue
        stock_files = os.listdir(batch_path)
        for stock_file in stock_files:
            stock_path = os.path.join(batch_path, stock_file)
            if not os.path.isfile(stock_path):
                continue
            stock_data = pd.read_csv(stock_path, encoding='utf-8')
            grouped = stock_data.groupby('symbol')
            for symbol, group_df in grouped:
                # 创建目标目录
                output_dir = os.path.join(archive_dir, symbol, dump_date)
                os.makedirs(output_dir, exist_ok=True)
                
                # 保存到目标目录
                output_file = os.path.join(output_dir, 'realtime_quotes.csv')
                # group_df对时间timestamp（2025-07-16 15:35:15）去重
                group_df = group_df.drop_duplicates(subset=['timestamp'])
                group_df = group_df.sort_values(by='timestamp')  # 按时间排序
                group_df.to_csv(output_file, index=False, encoding='utf-8')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract single stock data from dumped files.')
    parser.add_argument('--dump_dir', type=str, required=True, help='Directory containing dumped stock data files')
    parser.add_argument('--dump_date', type=str, required=True, help='Date of the dump in YYYY-MM-DD format')
    parser.add_argument('--archive_dir', type=str, required=True, help='Directory to save the extracted stock data')
    args = parser.parse_args()

    extract_single_stock_financial_data(args.dump_dir, args.dump_date, args.archive_dir)
    extract_single_stock_historical_data(args.dump_dir, args.dump_date, args.archive_dir)
    extract_single_stock_realtime_quotes(args.dump_dir, args.dump_date, args.archive_dir)