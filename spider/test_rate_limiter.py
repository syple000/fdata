import logging.config
import unittest
import asyncio
import time
from unittest.mock import patch, AsyncMock
import sys
import logging

from fdata.spider.rate_limiter import RateLimiter, RateLimiterManager


class TestRateLimiter(unittest.IsolatedAsyncioTestCase):
    """RateLimiter单元测试"""
    
    def setUp(self):
        """测试前的准备工作"""
        self.rate_limiter = RateLimiter()
    
    def test_init_default_values(self):
        """测试默认初始化值"""
        limiter = RateLimiter()
        self.assertEqual(limiter.max_concurrent, 1)
        self.assertEqual(limiter.min_interval, 1.0)
        self.assertEqual(limiter.max_requests_per_minute, 30)
        self.assertEqual(limiter._last_request_time, 0.0)
        self.assertEqual(len(limiter._request_times), 0)
    
    def test_init_custom_values(self):
        """测试自定义初始化值"""
        limiter = RateLimiter(
            max_concurrent=5,
            min_interval=2.0,
            max_requests_per_minute=60
        )
        self.assertEqual(limiter.max_concurrent, 5)
        self.assertEqual(limiter.min_interval, 2.0)
        self.assertEqual(limiter.max_requests_per_minute, 60)
    
    async def test_acquire_release_basic(self):
        """测试基本的获取和释放功能"""
        async with self.rate_limiter:
            # 在上下文中，信号量应该被获取
            self.assertEqual(self.rate_limiter._semaphore._value, 0)
        
        # 退出上下文后，信号量应该被释放
        self.assertEqual(self.rate_limiter._semaphore._value, 1)
    
    async def test_min_interval_enforcement(self):
        """测试最小间隔限制"""
        limiter = RateLimiter(min_interval=0.5)
        
        start_time = time.time()
        
        # 第一个请求
        async with limiter:
            pass
        
        # 第二个请求应该等待最小间隔
        async with limiter:
            pass
        
        elapsed = time.time() - start_time
        # 应该至少等待了最小间隔时间
        self.assertGreaterEqual(elapsed, 0.5)
    
    async def test_max_concurrent_requests(self):
        """测试最大并发请求限制"""
        limiter = RateLimiter(max_concurrent=2, min_interval=0.1)
        
        # 创建多个并发任务
        async def task():
            async with limiter:
                await asyncio.sleep(0.2)
        
        start_time = time.time()
        # 启动3个并发任务，但最多只能有2个同时运行
        await asyncio.gather(task(), task(), task())
        elapsed = time.time() - start_time
        
        # 由于最大并发为2，第三个任务需要等待，总时间应该大于0.2秒
        self.assertGreater(elapsed, 0.2)
    
    async def test_requests_per_minute_limit(self):
        """测试每分钟请求数限制"""
        limiter = RateLimiter(
            max_requests_per_minute=2,
            min_interval=0.0,
            max_concurrent=10
        )
        
        # 快速发送2个请求
        async with limiter:
            pass
        async with limiter:
            pass
        
        # 第三个请求应该被限制
        start_time = time.time()
        async with limiter:
            pass
        elapsed = time.time() - start_time
        
        # 应该等待了一段时间（接近60秒，但在测试中我们使用较小的值）
        self.assertGreater(elapsed, 55)
    
    async def test_request_times_cleanup(self):
        """测试请求时间记录的清理功能"""
        limiter = RateLimiter(max_requests_per_minute=5, min_interval=0.0)
        
        # 模拟过期的请求记录
        current_time = time.time()
        limiter._request_times = [
            current_time - 120,  # 2分钟前，应该被清理
            current_time - 30,   # 30秒前，应该保留
        ]
        
        async with limiter:
            pass
        
        # 检查过期记录是否被清理
        self.assertEqual(len(limiter._request_times), 2)  # 1个保留的 + 1个新的
        self.assertGreater(min(limiter._request_times), current_time - 60)


class TestRateLimiterManager(unittest.TestCase):
    """RateLimiterManager单元测试"""
    
    def setUp(self):
        """测试前的准备工作"""
        self.manager = RateLimiterManager()
    
    def test_init_default(self):
        """测试默认初始化"""
        manager = RateLimiterManager()
        self.assertEqual(len(manager.site_rate_limiters), 0)
        self.assertIsInstance(manager.default_rate_limiter, RateLimiter)
    
    def test_init_with_site_limiters(self):
        """测试使用站点限制器初始化"""
        limiters = {
            'example.com': RateLimiter(max_concurrent=2),
            'test.com': RateLimiter(max_concurrent=3)
        }
        manager = RateLimiterManager(limiters)
        self.assertEqual(len(manager.site_rate_limiters), 2)
        self.assertIn('example.com', manager.site_rate_limiters)
    
    def test_get_rate_limiter_exact_match(self):
        """测试精确匹配获取限制器"""
        custom_limiter = RateLimiter(max_concurrent=5)
        self.manager.add_rate_limiter('example.com', custom_limiter)
        
        result = self.manager.get_rate_limiter('example.com')
        self.assertEqual(result, custom_limiter)
    
    def test_get_rate_limiter_wildcard_match(self):
        """测试通配符匹配获取限制器"""
        custom_limiter = RateLimiter(max_concurrent=3)
        self.manager.add_rate_limiter('*.example.com', custom_limiter)
        
        result = self.manager.get_rate_limiter('api.example.com')
        self.assertEqual(result, custom_limiter)
        
        result = self.manager.get_rate_limiter('www.example.com')
        self.assertEqual(result, custom_limiter)
    
    def test_get_rate_limiter_default(self):
        """测试获取默认限制器"""
        result = self.manager.get_rate_limiter('unknown.com')
        self.assertEqual(result, self.manager.default_rate_limiter)
    
    def test_add_rate_limiter(self):
        """测试添加站点限制器"""
        custom_limiter = RateLimiter(max_concurrent=4)
        self.manager.add_rate_limiter('test.com', custom_limiter)
        
        self.assertIn('test.com', self.manager.site_rate_limiters)
        self.assertEqual(self.manager.site_rate_limiters['test.com'], custom_limiter)
    
    def test_remove_rate_limiter(self):
        """测试移除站点限制器"""
        custom_limiter = RateLimiter(max_concurrent=4)
        self.manager.add_rate_limiter('test.com', custom_limiter)
        
        # 确认添加成功
        self.assertIn('test.com', self.manager.site_rate_limiters)
        
        # 移除限制器
        self.manager.remove_rate_limiter('test.com')
        self.assertNotIn('test.com', self.manager.site_rate_limiters)
    
    def test_remove_nonexistent_rate_limiter(self):
        """测试移除不存在的站点限制器"""
        # 不应该抛出异常
        try:
            self.manager.remove_rate_limiter('nonexistent.com')
        except Exception as e:
            self.fail(f"移除不存在的限制器时抛出异常: {e}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main(verbosity=2)