
# 装饰器：统计执行时间并打印到日志
from functools import wraps
import logging
import time

def exec_time_cost(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"函数 '{func.__name__}' 执行时间: {end_time - start_time:.4f} 秒")
        return result
    return wrapper
