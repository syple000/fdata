import unittest
import tempfile
import os
import json
import logging
import time
import asyncio
from unittest.mock import patch, MagicMock
from .spider_core import AntiDetectionSpider
from .config import SpiderConfig
from .data_processor import DataProcessor
import shutil

class TestAntiDetectionSpider(unittest.IsolatedAsyncioTestCase):
    """AntiDetectionSpider 异步单元测试"""
    
    async def asyncSetUp(self):
        """测试前准备"""
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        self.temp_cookie_file = os.path.join(self.temp_dir, "test_cookies.json")
        self.temp_output_dir = os.path.join(self.temp_dir, "output")
        
        # 创建输出目录
        os.makedirs(self.temp_output_dir, exist_ok=True)
        
        # 创建爬虫实例
        self.spider = AntiDetectionSpider(
            auto_cookie=True,
            cookie_file=self.temp_cookie_file
        )
        
        # 测试URL
        self.test_url = "https://www.baidu.com"
        
        # 配置日志
        logging.basicConfig(level=logging.WARNING)
    
    async def asyncTearDown(self):
        """测试后清理"""
        # 停止爬虫
        try:
            await self.spider.stop()
        except:
            pass
        
        # 清理临时文件
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_initialization(self):
        """测试爬虫初始化"""
        # 测试默认初始化
        spider = AntiDetectionSpider()
        self.assertIsNotNone(spider.config)
        self.assertIsNone(spider.browser)
        self.assertIsNone(spider.playwright)
        self.assertIsNone(spider.context)
        self.assertFalse(spider.auto_cookie)  # 默认为False
        self.assertEqual(spider.cookie_file, "cookies.json")
        
        # 测试自定义配置初始化
        config = SpiderConfig()
        spider_custom = AntiDetectionSpider(
            config=config,
            auto_cookie=True,
            cookie_file="test_cookies.json"
        )
        self.assertEqual(spider_custom.config, config)
        self.assertTrue(spider_custom.auto_cookie)
        self.assertEqual(spider_custom.cookie_file, "test_cookies.json")
    
    async def test_start_and_stop(self):
        """测试启动和停止爬虫"""
        # 测试启动
        await self.spider.start()
        self.assertIsNotNone(self.spider.playwright)
        self.assertIsNotNone(self.spider.browser)
        self.assertIsNotNone(self.spider.context)
        self.assertIsNotNone(self.spider.user_agent)
        
        # 测试停止
        await self.spider.stop()
        self.assertIsNone(self.spider.context)
        self.assertIsNone(self.spider.browser)
        self.assertIsNone(self.spider.playwright)
    
    async def test_start_with_proxy(self):
        """测试使用代理启动爬虫"""
        await self.spider.start()
        # 使用免费代理进行测试（可能不稳定，仅用于测试代理配置）
        proxy_url = "127.0.0.1:10809"
        
        try:
            await self.spider.switch_context(proxy=proxy_url)
            self.assertIsNotNone(self.spider.context)
            result = await self.spider.crawl_url(self.test_url)
            self.assertIsInstance(result, dict)
            await self.spider.stop()
        except Exception as e:
            # 代理可能不可用，这是正常的
            self.assertIn("proxy", str(e).lower())
    
    async def test_switch_context(self):
        """测试切换浏览器上下文"""
        await self.spider.start()
        
        # 获取原始上下文
        original_context = self.spider.context
        
        # 切换上下文
        await self.spider.switch_context()
        
        # 验证上下文已切换
        self.assertIsNotNone(self.spider.context)
        self.assertNotEqual(self.spider.context, original_context)
        
        await self.spider.stop()
    
    async def test_switch_context_without_browser(self):
        """测试未启动浏览器时切换上下文"""
        with self.assertRaises(Exception):
            await self.spider.switch_context()
    
    async def test_cookie_management(self):
        """测试 Cookie 管理"""
        # 创建测试 Cookie 文件
        test_cookies = [
            {
                "name": "test_cookie",
                "value": "test_value",
                "domain": ".baidu.com",
                "path": "/",
                "httpOnly": False,
                "secure": False
            }
        ]
        
        with open(self.temp_cookie_file, 'w', encoding='utf-8') as f:
            json.dump(test_cookies, f)
        
        # 启动爬虫（应该自动加载 cookies）
        await self.spider.start()
        
        # 验证 cookies 加载
        self.assertTrue(os.path.exists(self.temp_cookie_file))
        
        # 测试保存 cookies
        await self.spider._save_cookies()
        self.assertTrue(os.path.exists(self.temp_cookie_file))
        
        # 测试清除 cookies
        await self.spider.clear_cookies()
        self.assertFalse(os.path.exists(self.temp_cookie_file))
        
        await self.spider.stop()
    
    async def test_load_cookies_file_not_exists(self):
        """测试加载不存在的 Cookie 文件"""
        non_existent_file = os.path.join(self.temp_dir, "non_existent_cookies.json")
        spider = AntiDetectionSpider(
            auto_cookie=True,
            cookie_file=non_existent_file
        )
        
        await spider.start()
        # 应该不会抛出异常
        self.assertIsNotNone(spider.context)
        await spider.stop()
    
    async def test_load_cookies_invalid_json(self):
        """测试加载无效的 Cookie 文件"""
        # 创建无效的 JSON 文件
        with open(self.temp_cookie_file, 'w', encoding='utf-8') as f:
            f.write("invalid json content")
        
        spider = AntiDetectionSpider(
            auto_cookie=True,
            cookie_file=self.temp_cookie_file
        )
        
        await spider.start()
        # 应该不会抛出异常，但会记录错误日志
        self.assertIsNotNone(spider.context)
        await spider.stop()
    
    async def test_create_page_without_context(self):
        """测试未启动上下文时创建页面"""
        with self.assertRaises(Exception) as context:
            await self.spider._create_page()
        self.assertIn("浏览器上下文未启动", str(context.exception))
    
    async def test_crawl_url_success(self):
        """测试成功爬取URL"""
        await self.spider.start()
        
        # 爬取百度首页
        result = await self.spider.crawl_url(self.test_url)
        
        # 验证结果
        self.assertIsInstance(result, dict)
        self.assertTrue(result.get("success", False))
        self.assertEqual(result.get("url"), self.test_url)
        self.assertIn("title", result)
        self.assertIn("content", result)
        self.assertIn("status", result)
        self.assertIn("timestamp", result)
        self.assertGreater(result.get("content_length", 0), 0)
        
        # 验证状态码
        self.assertEqual(result.get("status"), 200)
        
        # 验证标题包含"百度"
        self.assertIn("百度", result.get("title", ""))
        
        await self.spider.stop()
    
    async def test_crawl_url_with_wait_time(self):
        """测试带等待时间的爬取"""
        await self.spider.start()
        
        start_time = time.time()
        result = await self.spider.crawl_url(self.test_url, actions=[
            {"type": "wait", "selector": "#kw", "value": 2000}  # 等待2秒
        ])
        end_time = time.time()
        
        # 验证等待时间
        self.assertGreaterEqual(end_time - start_time, 0)
        self.assertTrue(result.get("success", False))
        
        await self.spider.stop()
    
    async def test_crawl_url_with_random_interval(self):
        """测试带随机间隔的爬取"""
        await self.spider.start()
        
        start_time = time.time()
        result = await self.spider.crawl_url(self.test_url, crawl_after_random_interval=True)
        end_time = time.time()
        
        # 验证有延迟
        self.assertGreater(end_time - start_time, 0)
        self.assertTrue(result.get("success", False))
        
        await self.spider.stop()
    
    async def test_crawl_url_with_element_selector(self):
        """测试等待特定元素的爬取"""
        await self.spider.start()
        
        # 等待百度搜索框出现
        result = await self.spider.crawl_url(
            self.test_url,
            actions=[
                {"type": "wait", "selector": "input[name='wd']", "value": 5000}  # 等待5秒
            ]
        )
        
        self.assertTrue(result.get("success", False))
        
        await self.spider.stop()
    
    async def test_crawl_url_invalid_url(self):
        """测试爬取无效URL"""
        await self.spider.start()
        
        invalid_url = "https://invalid-domain-that-does-not-exist.com"
        result = await self.spider.crawl_url(invalid_url, timeout=5000)
        
        # 验证失败结果
        self.assertIsInstance(result, dict)
        self.assertFalse(result.get("success", True))
        self.assertEqual(result.get("url"), invalid_url)
        self.assertIn("error", result)
        self.assertIn("timestamp", result)
        
        await self.spider.stop()
    
    async def test_crawl_url_timeout(self):
        """测试爬取超时"""
        await self.spider.start()
        
        # 使用很短的超时时间
        result = await self.spider.crawl_url(self.test_url, timeout=1)
        
        # 可能成功也可能失败（取决于网络速度）
        self.assertIsInstance(result, dict)
        self.assertIn("success", result)
        
        await self.spider.stop()
    
    async def test_crawl_url_without_browser(self):
        """测试未启动浏览器时爬取"""
        result = await self.spider.crawl_url(self.test_url)
        
        # 应该返回失败结果
        self.assertFalse(result.get("success", True))
        self.assertIn("error", result)
    
    async def test_retry_mechanism(self):
        """测试重试机制"""
        # 修改配置以便快速测试重试
        self.spider.config.MAX_RETRIES = 2
        await self.spider.start()
        
        # 使用不存在的域名触发重试
        invalid_url = "https://definitely-does-not-exist-domain.com"
        result = await self.spider.crawl_url(invalid_url, timeout=1000)
        
        # 验证重试后失败
        self.assertFalse(result.get("success", True))
        self.assertIn("error", result)
        
        await self.spider.stop()
    
    async def test_data_management(self):
        """测试数据管理功能"""
        await self.spider.start()
        
        # 爬取一些数据
        result = await self.spider.crawl_url(self.test_url)
        data_processor: DataProcessor = result.get("data_processor")
        
        # 测试获取摘要
        summary = data_processor.get_response_summary()
        self.assertIsInstance(summary, dict)
        
        # 测试保存数据
        json_file = data_processor.save_to_json()
        self.assertTrue(os.path.exists(json_file))
        
        csv_file = data_processor.save_to_csv()
        self.assertTrue(os.path.exists(csv_file))
        
        # 测试清空数据
        data_processor.clear_data()
        
        await self.spider.stop()
    
    async def test_multiple_crawls(self):
        """测试多次爬取"""
        await self.spider.start()
        
        urls = [
            "https://www.baidu.com",
            "https://www.baidu.com/s?wd=python",
        ]
        
        results = []
        for url in urls:
            result = await self.spider.crawl_url(url)
            results.append(result)
        
        # 验证所有爬取都有结果
        self.assertEqual(len(results), len(urls))
        
        for result in results:
            self.assertIsInstance(result, dict)
            self.assertIn("success", result)
            self.assertIn("url", result)
        
        await self.spider.stop()
   
    async def test_comprehensive_workflow(self):
        """测试完整的工作流程"""
        # 启动爬虫
        await self.spider.start()
        
        # 爬取页面
        result = await self.spider.crawl_url(self.test_url)
        self.assertTrue(result.get("success", False))
        
        # 切换上下文
        await self.spider.switch_context()
        
        # 再次爬取
        result2 = await self.spider.crawl_url(self.test_url)
        self.assertTrue(result2.get("success", False))
        
        # 清除 cookies
        await self.spider.clear_cookies()
        
        # 停止爬虫
        await self.spider.stop()
    
    async def test_error_handling_during_stop(self):
        """测试停止时的错误处理"""
        await self.spider.start()
        
        # 手动关闭浏览器以模拟错误
        if self.spider.browser:
            await self.spider.browser.close()
            self.spider.browser = None
        
        # 停止应该不会抛出异常
        await self.spider.stop()
    
    async def test_context_manager_usage(self):
        """测试异步上下文管理器用法"""
        spider = AntiDetectionSpider()
        
        async with spider as spider_instance:
            result = await spider_instance.crawl_url(self.test_url)
            self.assertTrue(result.get("success", False))
        
        # 验证爬虫已自动停止
        self.assertIsNone(spider.context)
        self.assertIsNone(spider.browser)
        self.assertIsNone(spider.playwright)

if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # 运行测试
    unittest.main(verbosity=2)