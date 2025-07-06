import time
import random
import asyncio
from typing import Optional, Dict, Any, List, Callable
from playwright.async_api import async_playwright, Page, Browser, Response, BrowserContext
from datetime import datetime
import logging
import os
import json

from .config import SpiderConfig
from .data_processor import DataProcessor, ResponseData
from ..utils.exec_time_cost import exec_time_cost


class AntiDetectionSpider:

    async def __aenter__(self):
        """支持异步上下文管理器"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """支持异步上下文管理器"""
        await self.stop()
    
    def __init__(self, config: Optional[SpiderConfig] = None, auto_cookie: bool = False, cookie_file: str = "cookies.json", proxy: str = None):
        self.config = config or SpiderConfig()
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.auto_cookie = auto_cookie
        self.cookie_file = cookie_file
        self.context: Optional[BrowserContext] = None
        self.proxy = proxy
        self._semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_REQUESTS if hasattr(self.config, 'MAX_CONCURRENT_REQUESTS') else 10)
    
    async def start(self):
        """启动爬虫"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(**self.config.BROWSER_CONFIG)
            
            # 创建浏览器上下文
            self.context = await self.browser.new_context(proxy={"server": self.proxy} if self.proxy else None)
            
            # 自动加载 cookie
            if self.auto_cookie:
                await self._load_cookies()
            
            self.user_agent = self.config.get_random_user_agent()

            logging.info("爬虫启动成功")
        except Exception as e:
            logging.error(f"爬虫启动失败: {e}")
            raise
    
    async def stop(self):
        """停止爬虫"""
        try:
            # 自动保存 cookie
            if self.auto_cookie and self.context:
                await self._save_cookies()
            
            if self.context:
                await self.context.close()
                self.context = None
            if self.browser:
                await self.browser.close()
                self.browser = None
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            logging.info("爬虫停止成功")
        except Exception as e:
            logging.error(f"爬虫停止失败: {e}")

    async def switch_context(self, proxy: str = None):    
        """切换到新的浏览器上下文"""
        try:
            # 保存 cookie
            if self.auto_cookie and self.context:
                await self._save_cookies()
 
            if self.context:
                await self.context.close()

            # 刷新proxy
            self.proxy = proxy

            self.context = await self.browser.new_context(proxy={"server": self.proxy} if self.proxy else None)

            # 加载 cookie
            if self.auto_cookie:
                await self._load_cookies()

            logging.info("成功切换浏览器上下文")
        except Exception as e:
            logging.error(f"切换浏览器上下文失败: {e}")
            raise
    
    async def _load_cookies(self):
        """加载 cookie"""
        if os.path.exists(self.cookie_file):
            try:
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                    await self.context.add_cookies(cookies)
                    logging.info(f"成功加载 {len(cookies)} 个 cookie")
            except Exception as e:
                logging.error(f"加载 cookie 失败: {e}")
    
    async def _save_cookies(self):
        """保存 cookie"""
        try:
            cookies = await self.context.cookies()
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, indent=2, ensure_ascii=False)
            logging.info(f"成功保存 {len(cookies)} 个 cookie")
        except Exception as e:
            logging.error(f"保存 cookie 失败: {e}")
    
    async def clear_cookies(self):
        """清除所有 cookie"""
        try:
            if self.context:
                await self.context.clear_cookies()
            if os.path.exists(self.cookie_file):
                os.remove(self.cookie_file)
            logging.info("成功清除所有 cookie")
        except Exception as e:
            logging.error(f"清除 cookie 失败: {e}")
  
    async def _create_page(self) -> Page:
        """创建新页面并配置反检测"""
        if not self.context:
            raise Exception("浏览器上下文未启动")
        
        # 创建新页面
        page = await self.context.new_page()
        
        # 设置视口
        await page.set_viewport_size(self.config.PAGE_CONFIG['viewport'])
        
        # 设置User-Agent
        await page.set_extra_http_headers({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # 注入反检测脚本
        await page.add_init_script("""
            // 移除webdriver标识
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
            });
            
            // 修改plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => ({
                    length: 2,
                    0: { name: "Chrome PDF Plugin", filename: "internal-pdf-viewer", description: "Portable Document Format" },
                    1: { name: "Chrome PDF Viewer", filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai", description: "" }
                }),
            });
            
            // 修改languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['zh-CN', 'zh', 'en'],
            });
            
            // 修改permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)
        
        return page
    
    def _setup_response_listener(self, page: Page, data_processor: DataProcessor) -> List[ResponseData]:
        """设置响应监听器"""
        responses = []
        
        async def handle_response(response: Response):
            try:
                start_time = time.time()
                
                # 获取响应头
                headers = response.headers
                
                # 获取响应体
                try:
                    body = await response.body()
                except Exception:
                    body = b""
                
                # 创建响应数据
                response_data = ResponseData(
                    url=response.url,
                    status=response.status,
                    content_type=headers.get('content-type', ''),
                    headers=dict(headers),
                    timestamp=datetime.now().isoformat(),
                    size=len(body) if body else 0,
                    response_time=time.time() - start_time,
                    body=body.hex() if body else "",
                )
                
                responses.append(response_data)
                data_processor.add_response(response_data)
                
                # 记录日志
                logging.info(f"响应捕获: {response.status} {response.url}")
                
            except Exception as e:
                logging.error(f"处理响应失败: {e}")
        
        page.on("response", handle_response)
        return responses
    
    @exec_time_cost
    async def crawl_url(self, url: str,
        crawl_after_random_interval: bool = False, 
        timeout: int = 30000, 
        output_dir: str = "output", 
        filter_func: Optional[Callable[..., bool]] = None,
        wait_elem_selector: str = None, 
        wait_time: int = 0) -> Dict[str, Any]:

        """爬取单个URL"""
        async with self._semaphore:  # 限制并发数
            logging.info(f"开始爬取: {url}")

            data_processor = DataProcessor(output_dir=output_dir, filter_func=filter_func)
            
            retry_count = 0
            max_retries = self.config.MAX_RETRIES
            
            while retry_count < max_retries:
                try:
                    # 创建新页面
                    page = await self._create_page()
                    
                    # 设置响应监听
                    responses = self._setup_response_listener(page, data_processor)
                    
                    # 随机延迟
                    if crawl_after_random_interval:
                        delay = self.config.get_random_delay()
                        logging.info(f"等待 {delay:.2f} 秒后开始请求")
                        await asyncio.sleep(delay)
                    
                    # 访问页面
                    response = await page.goto(url, timeout=timeout)
                    
                    # 等待页面加载或指定元素出现
                    if wait_elem_selector and wait_time > 0:
                        await page.wait_for_selector(wait_elem_selector, timeout=wait_time)
                    elif wait_time > 0:
                        await page.wait_for_timeout(wait_time)
                    
                    # 获取页面信息
                    title = await page.title()
                    content = await page.content()
                    
                    # 关闭页面
                    await page.close()
                    
                    result = {
                        "url": url,
                        "title": title,
                        "content": content,
                        "status": response.status if response else 0,
                        "responses_count": len(responses),
                        "content_length": len(content),
                        "timestamp": datetime.now().isoformat(),
                        "success": True,
                        "data_processor": data_processor,
                    }
                    
                    logging.info(f"爬取成功: {url} - 状态码: {result['status']}")
                    return result
                    
                except Exception as e:
                    retry_count += 1
                    logging.error(f"爬取失败 (重试 {retry_count}/{max_retries}): {url} - {e}")
                    
                    if retry_count < max_retries:
                        # 重试前等待
                        await asyncio.sleep(random.uniform(2, 5))
                    else:
                        return {
                            "url": url,
                            "error": str(e),
                            "timestamp": datetime.now().isoformat(),
                            "success": False
                        }
            
            return {"url": url, "success": False, "error": "Max retries exceeded"}
    
    async def crawl_urls(self, urls: List[str], **kwargs) -> List[Dict[str, Any]]:
        """并发爬取多个URL"""
        tasks = []
        for url in urls:
            task = asyncio.create_task(self.crawl_url(url, **kwargs))
            tasks.append(task)
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理异常结果
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "url": urls[i],
                    "error": str(result),
                    "timestamp": datetime.now().isoformat(),
                    "success": False
                })
            else:
                processed_results.append(result)
        
        return processed_results
  