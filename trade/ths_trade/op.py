from pywinauto import application

from ..models import *


# 执行同花顺UI下单撤单、仓位资金查询
class THSTradeOp:
    def __init__(self, xiadan_exe_path: str):
        self.app = application.Application(backend="uia").start(xiadan_exe_path)

    def place_order(self, order: Order):
        pass

    def cancel_order(self, order: Order):
        """
        撤单操作
        :param order_id: 订单ID
        """
        # 实现撤单逻辑
        pass

    def query_position(self):
        """
        查询仓位信息
        """
        # 实现查询仓位逻辑
        pass

    def query_funds(self):
        """
        查询资金信息
        """
        # 实现查询资金逻辑
        pass

if __name__ == '__main__':
    # 示例：初始化同花顺下单操作
    ths_trade_op = THSTradeOp(xiadan_exe_path="C:\\Path\\To\\Xiadan.exe")