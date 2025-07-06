import time
import random
from typing import Optional, Dict, Any, List, Callable
from playwright.sync_api import sync_playwright, Page, Browser, Response
from datetime import datetime
import logging
import os
import json

from .config import SpiderConfig
from .data_processor import DataProcessor, ResponseData
from ..utils.exec_time_cost import exec_time_cost


class AntiDetectionSpider:
    """反检测爬虫类"""
    
    def __init__(self, config: Optional[SpiderConfig] = None, output_dir: str = "output", filter_func: Optional[Callable[..., bool]] = None, auto_cookie: bool = False, cookie_file: str = "cookies.json"):
        self.config = config or SpiderConfig()
        self.data_processor = DataProcessor(output_dir=output_dir, filter_func=filter_func)
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.auto_cookie = auto_cookie
        self.cookie_file = cookie_file
        self.context = None
    
    def start(self, proxy: str = None):
        """启动爬虫"""
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(**self.config.BROWSER_CONFIG)
            
            # 创建浏览器上下文
            self.context = self.browser.new_context(proxy={"server": proxy} if proxy else None)
            
            # 自动加载 cookie
            if self.auto_cookie:
                self._load_cookies()
            
            self.user_agent = self.config.get_random_user_agent()

            logging.info("爬虫启动成功")
        except Exception as e:
            logging.error(f"爬虫启动失败: {e}")
            raise
    
    def stop(self):
        """停止爬虫"""
        try:
            # 自动保存 cookie
            if self.auto_cookie and self.context:
                self._save_cookies()
            
            if self.context:
                self.context.close()
                self.context = None
            if self.browser:
                self.browser.close()
                self.browser = None
            if self.playwright:
                self.playwright.stop()
                self.playwright = None
            logging.info("爬虫停止成功")
        except Exception as e:
            logging.error(f"爬虫停止失败: {e}")

    def switch_context(self, proxy: str = None):    
        """切换到新的浏览器上下文"""
        try:
            # 保存 cookie
            if self.auto_cookie and self.context:
                self._save_cookies()
 
            if self.context:
                self.context.close()

            self.context = self.browser.new_context(proxy={"server": proxy} if proxy else None)

            # 加载 cookie
            if self.auto_cookie:
                self._load_cookies()

            logging.info("成功切换浏览器上下文")
        except Exception as e:
            logging.error(f"切换浏览器上下文失败: {e}")
            raise
    
    def _load_cookies(self):
        """加载 cookie"""
        if os.path.exists(self.cookie_file):
            try:
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                    self.context.add_cookies(cookies)
                    logging.info(f"成功加载 {len(cookies)} 个 cookie")
            except Exception as e:
                logging.error(f"加载 cookie 失败: {e}")
    
    def _save_cookies(self):
        """保存 cookie"""
        try:
            cookies = self.context.cookies()
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, indent=2, ensure_ascii=False)
            logging.info(f"成功保存 {len(cookies)} 个 cookie")
        except Exception as e:
            logging.error(f"保存 cookie 失败: {e}")
    
    def clear_cookies(self):
        """清除所有 cookie"""
        try:
            if self.context:
                self.context.clear_cookies()
            if os.path.exists(self.cookie_file):
                os.remove(self.cookie_file)
            logging.info("成功清除所有 cookie")
        except Exception as e:
            logging.error(f"清除 cookie 失败: {e}")
  
    def _create_page(self) -> Page:
        """创建新页面并配置反检测"""
        if not self.context:
            raise Exception("浏览器上下文未启动")
        
        # 创建新页面
        page = self.context.new_page()
        
        # 设置视口
        page.set_viewport_size(self.config.PAGE_CONFIG['viewport'])
        
        # 设置随机User-Agent
        page.set_extra_http_headers({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # 注入反检测脚本
        page.add_init_script("""
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
    
    def _setup_response_listener(self, page: Page) -> List[ResponseData]:
        """设置响应监听器"""
        responses = []
        
        def handle_response(response: Response):
            try:
                start_time = time.time()
                
                # 获取响应头
                headers = response.headers
                
                # 创建响应数据
                response_data = ResponseData(
                    url=response.url,
                    status=response.status,
                    content_type=headers.get('content-type', ''),
                    headers=dict(headers),
                    timestamp=datetime.now().isoformat(),
                    size=len(response.body()) if response.body() else 0,
                    response_time=time.time() - start_time,
                    body=response.body().hex() if response.body() else "",
                )
                
                responses.append(response_data)
                self.data_processor.add_response(response_data)
                
                # 记录日志
                logging.info(f"响应捕获: {response.status} {response.url}")
                
            except Exception as e:
                logging.error(f"处理响应失败: {e}")
        
        page.on("response", handle_response)
        return responses
    
    @exec_time_cost
    def crawl_url(self, url: str, crawl_after_random_interval: bool = False, timeout: int = 30000, wait_elem_selector: str = None, wait_time: int = 0) -> Dict[str, Any]:
        """爬取单个URL"""
        logging.info(f"开始爬取: {url}")
        
        retry_count = 0
        max_retries = self.config.MAX_RETRIES
        
        while retry_count < max_retries:
            try:
                # 创建新页面
                page = self._create_page()
                
                # 设置响应监听
                responses = self._setup_response_listener(page)
                
                # 随机延迟
                if crawl_after_random_interval:
                    delay = self.config.get_random_delay()
                    logging.info(f"等待 {delay:.2f} 秒后开始请求")
                    time.sleep(delay)
                
                # 访问页面
                response = page.goto(url, timeout=timeout)
                # 等待页面加载或指定元素出现
                if wait_elem_selector and wait_time > 0:
                    page.wait_for_selector(wait_elem_selector, timeout=wait_time)
                elif wait_time > 0:
                    page.wait_for_timeout(wait_time)
                
                # 获取页面信息
                title = page.title()
                content = page.content()
                
                # 关闭页面
                page.close()
                
                result = {
                    "url": url,
                    "title": title,
                    "content": content,
                    "status": response.status if response else 0,
                    "responses_count": len(responses),
                    "content_length": len(content),
                    "timestamp": datetime.now().isoformat(),
                    "success": True
                }
                
                logging.info(f"爬取成功: {url} - 状态码: {result['status']}")
                return result
                
            except Exception as e:
                retry_count += 1
                logging.error(f"爬取失败 (重试 {retry_count}/{max_retries}): {url} - {e}")
                
                if retry_count < max_retries:
                    # 重试前等待
                    time.sleep(random.uniform(2, 5))
                else:
                    return {
                        "url": url,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                        "success": False
                    }
        
        return {"url": url, "success": False, "error": "Max retries exceeded"}
    
    def get_summary(self) -> Dict[str, Any]:
        """获取爬取摘要"""
        return self.data_processor.get_response_summary()
    
    def save_data(self, format_type: str = "json") -> str:
        """保存数据"""
        if format_type.lower() == "json":
            return self.data_processor.save_to_json()
        elif format_type.lower() == "csv":
            return self.data_processor.save_to_csv()
        else:
            raise ValueError("支持的格式: json, csv")
    
    def clear_data(self):
        """清空数据"""
        self.data_processor.clear_data()
        logging.info("数据已清空")
