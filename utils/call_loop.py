
# 调用循环，传入函数和间隔时间，无限调用直到检查函数为False退出
import asyncio
async def async_call_loop(func, *args, interval=1.0, check_func=None, ignore_exceptions=False, **kwargs):
    """
    无限循环调用指定函数，直到检查函数返回False。
    
    :param func: 要调用的函数
    :param interval: 调用间隔时间（秒）
    :param check_func: 检查函数，返回False时停止循环
    """
    while True:
        if check_func and not check_func():
            break  # 如果检查函数返回False，则退出循环
        
        try:
            await func(*args, **kwargs)  # 调用指定的异步函数
        except Exception as e:
            if ignore_exceptions:
                print(f"ignore exception {e}")
            else:
                raise e
        
        await asyncio.sleep(interval)  # 等待指定的间隔时间