import random
from typing import List

class SpiderConfig:
    """爬虫配置类"""

    # 反爬虫配置
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
    ]
    
    # 请求延迟配置（秒）
    MIN_DELAY = 1
    MAX_DELAY = 3
    
    # 重试配置
    MAX_RETRIES = 1

    # action interval
    ACTION_INTERVAL = 0.5  # 每次操作间隔时间（秒）
    
    # 浏览器配置
    BROWSER_CONFIG = {
        'headless': True,
        'slow_mo': 0,
        'args': [
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
        ]
    }
    
    # 页面配置
    PAGE_CONFIG = {
        'viewport': {'width': 1920, 'height': 1080},
        'ignore_https_errors': True,
        'java_script_enabled': True,
    }
    
    @classmethod
    def get_random_user_agent(cls) -> str:
        """获取随机User-Agent"""
        return random.choice(cls.USER_AGENTS)
    
    @classmethod
    def get_random_delay(cls) -> float:
        """获取随机延迟时间"""
        return random.uniform(cls.MIN_DELAY, cls.MAX_DELAY)
    
    @classmethod
    def get_random_proxy(cls) -> str:
        """获取随机代理"""
        return random.choice(cls.PROXIES) if cls.PROXIES else None
