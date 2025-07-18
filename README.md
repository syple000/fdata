## 使用说明
### 抓取频率说明
1. 新浪实时行情: 1s抓取一次，一次最多抓取100只股票
2. 历史行情/财务数据: 5s抓取一次，一次最多抓取1只股票的一个页面
### 启动dumper
共6类dumper：stock_list,realtime, historical, financial, stock_quote, dividend_info
1. stock_list: 获取市场股票列表
2. realtime: 获取实时行情推送
3. historical: 获取历史行情数据
4. financial: 获取财务数据
5. stock_quote: 获取股票报价信息
6. dividend_info: 获取除权除息分红配股信息

启动方式
执行入口在fdata/market_data_dumper.py下，可以根据参数自定义启动命令。如果想获取默认执行命令，执行fdata/scripts/echo_dump_cmd.py脚本

### 股票数据归档
支持对已抓取数据归档（归档会做各股票数据去重）。脚本位置fdata/scripts/archive.py

### 股票kline展示
脚本位置show_kline_pic.py