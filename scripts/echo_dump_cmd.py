from datetime import datetime
import subprocess as sub

cmd = '''
conda run --live-stream -n {env} \
python -m fdata.market_data.market_data_dumper \
{functions} \
--directory output \
--symbols {symbols} \
--duration {duration} \
--start_date {start_date} \
--kline_type {kline_type} \
--end_date {today} \
--adjust_type {adjust_type} \
--today_date {today} \
'''

def echo_sub_exec(cmd, timeout=None):
    process = sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.STDOUT, text=True, bufsize=1, shell=True)
    for line in iter(process.stdout.readline, ''):
        print(line, end='', flush=True)

    process.stdout.close()
    retcode = process.wait(timeout)
    if retcode != 0:
        raise Exception(f"retcode: {retcode} != 0")

if __name__ == '__main__': # 只执行股票相关的dump功能；仅做命令的输出
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description='Run market data dumper with specified parameters.')
    parser.add_argument('--env', type=str, default='dev', help='Conda environment name')
    parser.add_argument('--duration', type=int, default=0, help='Duration in seconds for realtime data fetching')
    parser.add_argument('--start_date', type=str, default='2001-01-01', help='Start date for historical data (YYYY-MM-DD)')
    parser.add_argument('--kline_type', type=str, default='daily', help='K-line type for historical data')
    parser.add_argument('--today', type=str, default=datetime.now().strftime('%Y-%m-%d'), help='Today date in YYYY-MM-DD format')
    parser.add_argument('--adjust_type', type=str, default='forward', help='Adjust type for historical data')

    parser.add_argument('functions', type=str, help='Functions to run (e.g., financial,historical)')
    parser.add_argument('stock_list_path', type=str, help='Path to stock list CSV file') # functions不是stock_list时，必传
    args = parser.parse_args()

    if args.duration <= 0:
        args.duration = int(datetime.strptime(args.today + ' ' + '15:30:00', '%Y-%m-%d %H:%M:%S').timestamp() - datetime.now().timestamp())
    
    stock_list = pd.read_csv(args.stock_list_path, dtype=str)
    symbols = stock_list['symbol'].tolist()
    cmd = cmd.format(
        env=args.env,
        functions=args.functions,
        symbols=','.join(symbols),
        duration=args.duration,
        start_date=args.start_date,
        kline_type=args.kline_type,
        adjust_type=args.adjust_type,
        today=args.today,
    )
    print(cmd)
        