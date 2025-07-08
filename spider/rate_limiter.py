import asyncio
import time
from typing import Dict, List
from collections import defaultdict
import logging


class RateLimiter:
    """站点流控器"""
    
    def __init__(self, 
                 max_concurrent: int = 1,
                 min_interval: float = 1.0,
                 max_requests_per_minute: int = 30):
        """
        初始化流控器
        
        Args:
            max_concurrent: 最大并发请求数
            min_interval: 最小请求间隔(秒)
            max_requests_per_minute: 每分钟最大请求数
        """
        self.max_concurrent = max_concurrent
        self.min_interval = min_interval
        self.max_requests_per_minute = max_requests_per_minute
        
        # 并发控制信号量
        self._semaphore = asyncio.Semaphore(max_concurrent)
        
        # 最后请求时间
        self._last_request_time = 0.0
        
        # 请求时间统计（用于频率控制）
        self._request_times: List[float] = []
        
        # 锁，用于保护共享状态
        self._lock = asyncio.Lock()
    
    async def _acquire(self):
        """内部获取请求许可"""
        # 获取并发许可
        await self._semaphore.acquire()
        
        try:
            async with self._lock:
                current_time = time.time()
                
                # 1. 检查最小间隔
                if self._last_request_time > 0:
                    elapsed = current_time - self._last_request_time
                    if elapsed < self.min_interval:
                        wait_time = self.min_interval - elapsed
                        logging.info(f"等待 {wait_time:.2f} 秒以满足最小间隔要求")
                        await asyncio.sleep(wait_time)
                        current_time = time.time()
                
                # 2. 检查频率限制
                if self.max_requests_per_minute > 0:
                    # 清理1分钟前的请求记录
                    cutoff_time = current_time - 60
                    self._request_times = [t for t in self._request_times if t > cutoff_time]
                    
                    # 检查是否超过频率限制
                    if len(self._request_times) >= self.max_requests_per_minute:
                        oldest_request = min(self._request_times)
                        wait_time = 60 - (current_time - oldest_request)
                        if wait_time > 0:
                            logging.info(f"等待 {wait_time:.2f} 秒以满足频率限制")
                            await asyncio.sleep(wait_time)
                            current_time = time.time()
                
                # 更新请求时间
                self._last_request_time = current_time
                self._request_times.append(current_time)
                
        except Exception as e:
            raise e
    
    def _release(self):
        """内部释放请求许可"""
        self._semaphore.release()
    
    async def __aenter__(self):
        """支持异步上下文管理器"""
        await self._acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """支持异步上下文管理器"""
        self._release()


class RateLimiterManager:
    """流控管理器"""
    
    def __init__(self, site_rate_limiters: Dict[str, RateLimiter] = None):
        """
        初始化流控管理器
        
        Args:
            site_rate_limiters: 站点到流控器的映射
        """
        self.site_rate_limiters = site_rate_limiters or {}
        
        # 默认流控器
        self.default_rate_limiter = RateLimiter()
    
    def get_rate_limiter(self, host: str) -> RateLimiter:
        """获取指定host的流控器"""
        # 优先匹配具体host
        if host in self.site_rate_limiters:
            return self.site_rate_limiters[host]
        
        # 匹配域名模式（例如 *.example.com）
        for pattern, rate_limiter in self.site_rate_limiters.items():
            if pattern.startswith('*.'):
                domain = pattern[2:]  # 去掉 '*.'
                if host.endswith(domain):
                    return rate_limiter
        
        # 使用默认流控器
        return self.default_rate_limiter
    
    def add_rate_limiter(self, host: str, rate_limiter: RateLimiter):
        """添加站点流控器"""
        self.site_rate_limiters[host] = rate_limiter
    
    def remove_rate_limiter(self, host: str):
        """移除站点流控器"""
        if host in self.site_rate_limiters:
            del self.site_rate_limiters[host]