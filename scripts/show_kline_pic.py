import argparse
import mplfinance as mpf
import os
import pandas as pd

if __name__ == '__main__':
    # 展示kline数据图，从archive/{symbol}/{dump_date}/{date_range}/下读取
    parser = argparse.ArgumentParser(description='Show K-line chart from dumped data.')
    parser.add_argument('--symbol', type=str, required=True, help='Stock symbol to show K-line chart for')
    parser.add_argument('--archive_dir', type=str, default='archive', help='Base directory for archived stock data')
    parser.add_argument('--dump_date', type=str, default='', help='Date of the dump in YYYY-MM-DD format')
    parser.add_argument('--date_range', type=str, default='', help='Date range for the K-line chart')
    parser.add_argument('--kline_type', type=str, default='daily', help='K-line type (e.g., daily, weekly, monthly)')
    parser.add_argument('--adjust_type', type=str, default='forward', help='Adjust type for historical data')

    args = parser.parse_args()

    # 构建数据路径，如果args.dump_date空，就寻找字典序最大的dump_date; 同理dump_range也是一样
    symbol_dir = os.path.join(args.archive_dir, args.symbol)
    if not os.path.exists(symbol_dir):
        raise FileNotFoundError(f"Symbol directory not found: {symbol_dir}")

    dump_dates = sorted(os.listdir(symbol_dir))
    if not dump_dates:
        raise FileNotFoundError(f"No dump dates found in: {symbol_dir}")

    dump_date = args.dump_date if args.dump_date else dump_dates[-1]
    dump_date_dir = os.path.join(symbol_dir, dump_date)
    if not os.path.exists(dump_date_dir):
        raise FileNotFoundError(f"Dump date directory not found: {dump_date_dir}")

    date_ranges = sorted(os.listdir(dump_date_dir))
    # 过滤形如YYYY-mm-dd_YYYY-mm-dd的目录
    date_ranges = [dr for dr in date_ranges if len(dr.split('_')) == 2 and all(part.isdigit() for part in dr.replace('-', '_').split('_'))]
    if not date_ranges:
        raise FileNotFoundError(f"No date ranges found in: {dump_date_dir}")

    date_range = args.date_range if args.date_range else date_ranges[-1]
    data_path = os.path.join(dump_date_dir, date_range)
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data path not found: {data_path}")

    print(f"Using data path: {data_path}")

    path = os.path.join(data_path, f'historical_data_{args.kline_type}_{args.adjust_type}.csv')
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")

    # Read the CSV file with appropriate column types
    df = pd.read_csv(
        path,
        encoding='utf-8',
        dtype={
            'symbol': str,
            'date': str,
            'open_price': float,
            'high_price': float,
            'low_price': float,
            'close_price': float,
            'volume': int
        }
    )

    # Rename columns to match mplfinance requirements
    df.rename(
        columns={
            'date': 'Date',
            'open_price': 'Open',
            'high_price': 'High',
            'low_price': 'Low',
            'close_price': 'Close',
            'volume': 'Volume'
        },
        inplace=True
    )

    # Convert 'Date' column to datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # Set 'Date' as the index
    df.set_index('Date', inplace=True)

    # Save the K-line chart as an image using mplfinance
    mpf.plot(df, type='line', volume=True, title=f"K-line chart for {args.symbol}", savefig=f"{args.symbol}_kline_chart.png")
