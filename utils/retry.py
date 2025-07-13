# 重试装饰器
import asyncio
import time
import logging

def retry(max_retries: int = 3, delay: float = 1.0, ignore_exceptions: bool = False):
    """
    重试装饰器，用于在函数执行失败时进行重试
    :param max_retries: 最大重试次数
    :param delay: 重试间隔时间（秒）
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries or max_retries <= 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries and max_retries > 0:
                        if ignore_exceptions:
                            logging.error(f"Error: {e}. Ignoring after {retries} retries.")
                            return None
                        else:
                            raise e
                    logging.error(f"Error: {e}. Retrying {retries}/{max_retries} in {delay} seconds...")
                    time.sleep(delay)
        return wrapper
    return decorator

def async_retry(max_retries: int = 3, delay: float = 1.0, ignore_exceptions: bool = False):
    """
    重试装饰器，用于在函数执行失败时进行重试
    :param max_retries: 最大重试次数
    :param delay: 重试间隔时间（秒）
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries or max_retries <= 0:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries and max_retries > 0:
                        if ignore_exceptions:
                            logging.error(f"Error: {e}. Ignoring after {retries} retries.")
                            return None
                        else:
                            raise e
                    logging.error(f"Error: {e}. Retrying {retries}/{max_retries} in {delay} seconds...")
                    await asyncio.sleep(delay)
        return wrapper
    return decorator