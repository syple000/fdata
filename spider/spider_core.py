import time
import random
import asyncio
from typing import Optional, Dict, Any, List, Callable
from playwright.async_api import async_playwright, Page, Browser, Response, BrowserContext
from playwright_stealth import Stealth
from datetime import datetime
import logging
import os
import json

from .config import SpiderConfig
from .data_processor import DataProcessor, ResponseData
from ..utils.exec_time_cost import exec_time_cost
from ..utils.bytes_str_convert import from_bytes_to_str, from_str_to_bytes
from dataclasses import dataclass

@dataclass
class CrawlResult:
    """爬取结果数据结构"""
    url: str
    success: bool
    timestamp: str
    title: Optional[str] = None
    content: Optional[str] = None
    status: Optional[int] = None
    responses_count: int = 0
    content_length: int = 0
    error: Optional[str] = None
    data_processor: Optional[DataProcessor] = None



class AntiDetectionSpider:

    async def __aenter__(self):
        """支持异步上下文管理器"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """支持异步上下文管理器"""
        await self.stop()
    
    def __init__(self, config: Optional[SpiderConfig] = None, auto_cookie: bool = False, cookie_file: str = "cookies.json", proxy: str = None, max_concurrent_requests: int = 20):
        self.config = config or SpiderConfig()
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.auto_cookie = auto_cookie
        self.cookie_file = cookie_file
        self.context: Optional[BrowserContext] = None
        self.proxy = proxy
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
    
    async def start(self):
        """启动爬虫"""
        try:
            stealth = Stealth()

            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(**self.config.BROWSER_CONFIG)
            
            # 创建浏览器上下文
            self.context = await self.browser.new_context(proxy={"server": self.proxy} if self.proxy else None)
            await stealth.apply_stealth_async(self.context)  # 启用反检测脚本
            
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

            stealth = Stealth()
            self.context = await self.browser.new_context(proxy={"server": self.proxy} if self.proxy else None)
            await stealth.apply_stealth_async(self.context)  # 启用反检测脚本

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
  
    async def _create_page(self, headers: Optional[Dict[str, str]] = None) -> Page:
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
        if headers:
            await page.set_extra_http_headers(headers)
        
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
                    body=from_bytes_to_str(body),
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
        headers: Optional[Dict[str, str]] = None,
        crawl_after_random_interval: bool = False, 
        timeout: int = 30000, 
        output_dir: str = "output", 
        filter_func: Optional[Callable[[ResponseData], bool]] = None,
        actions: Optional[List[Dict[str, Any]]] = None,
    ) -> CrawlResult:
        '''
        actions eg: [
            {"type": "click", "selector": "#login-button"},
            {"type": "input", "selector": "#username", "value": "my_username"},
            {"type": "input", "selector": "#password", "value": "my_password"},
            {"type": "click", "selector": "#submit-button"},
            {"type": "wait", "selector": "#dashboard", "value": 5000}
        ]
        '''

        """爬取单个URL"""
        async with self._semaphore:  # 限制并发数
            logging.info(f"开始爬取: {url}")

            data_processor = DataProcessor(output_dir=output_dir, filter_func=filter_func)
            
            retry_count = 0
            max_retries = self.config.MAX_RETRIES
            
            while retry_count < max_retries:
                try:
                    # 创建新页面
                    page = await self._create_page(headers)
                    
                    # 设置响应监听
                    responses = self._setup_response_listener(page, data_processor)
                    
                    # 随机延迟
                    if crawl_after_random_interval:
                        delay = self.config.get_random_delay()
                        logging.info(f"等待 {delay:.2f} 秒后开始请求")
                        await asyncio.sleep(delay)
                    
                    # 访问页面
                    response = await page.goto(url, timeout=timeout)
                    
                    # execute actions if provided
                    if actions:
                        for action in actions:
                            if action['type'] == 'click':
                                await page.click(action['selector'])
                            elif action['type'] == 'input':
                                await page.fill(action['selector'], action['value'])
                            elif action['type'] == 'wait':
                                wait_elem_selector = action.get('selector')
                                wait_time = action.get('value', 0)
                                await page.wait_for_selector(wait_elem_selector, timeout=wait_time)
                            else:
                                logging.warning(f"未知操作类型: {action['type']}")

                            await asyncio.sleep(self.config.ACTION_INTERVAL)  # 每次操作间隔时间
                    
                    # 获取页面信息
                    title = await page.title()
                    content = await page.content()
                    
                    # 关闭页面
                    await page.close()
                    
                    result = CrawlResult(
                        url=url,
                        success=True,
                        timestamp=datetime.now().isoformat(),
                        title=title,
                        content=content,
                        status=response.status if response else 0,
                        responses_count=len(responses),
                        content_length=len(content),
                        data_processor=data_processor
                    )
                    
                    logging.info(f"爬取成功: {url} - 状态码: {result.status}")
                    return result
                    
                except Exception as e:
                    retry_count += 1
                    logging.error(f"爬取失败 (重试 {retry_count}/{max_retries}): {url} - {e}")
                    
                    if retry_count < max_retries:
                        # 重试前等待
                        await asyncio.sleep(random.uniform(2, 5))
                    else:
                        return CrawlResult(
                            url=url,
                            success=False,
                            timestamp=datetime.now().isoformat(),
                            error=str(e)
                        )
            
            return CrawlResult(
                url=url,
                success=False,
                timestamp=datetime.now().isoformat(),
                error="Max retries exceeded"
            )