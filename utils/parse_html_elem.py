from typing import Optional
from bs4 import BeautifulSoup

# 使用 BeautifulSoup 解析 HTML
def extract_content(html_content: str, path: str) -> Optional[str]:
    soup = BeautifulSoup(html_content, "html.parser")
    element = soup.select_one(path)  # 使用 CSS 选择器查找元素
    return element.text.strip() if element else None

if __name__ == "__main__":
    # 示例 HTML 内容
    html_content = """
    <html>
        <body>
            <div class="container">
                <pre>Example content</pre>
            </div>
        </body>
    </html>
    """
    
    # 提取内容
    path = "div.container > pre"
    element = extract_content(html_content, path)
    
    assert element == 'Example content', "提取的内容不正确"