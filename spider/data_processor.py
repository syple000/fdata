import json
import csv
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
import os

@dataclass
class ResponseData:
    """响应数据结构"""
    url: str
    status: int
    content_type: str
    headers: Dict[str, str]
    timestamp: str
    size: int = 0
    response_time: float = 0.0
    body: str = ""  # bytes.hex()得到；可以通过bytes.fromhex()转换回bytes

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)

class DataProcessor:
    """数据处理类"""
    
    def __init__(self, output_dir: str = "output", filter_func: Optional[Callable[..., bool]] = None):
        self.output_dir = output_dir
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.filter_func = filter_func
        self.responses: List[ResponseData] = []
        self._ensure_output_dir()
    
    def _ensure_output_dir(self):
        """确保输出目录存在"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def add_response(self, response_data: ResponseData):
        """添加响应数据"""
        if self.filter_func and not self.filter_func(response_data):
            return  # 如果有过滤函数且不通过，则不添加
        self.responses.append(response_data)
    
    def get_response_summary(self) -> Dict[str, Any]:
        """获取响应摘要"""
        if not self.responses:
            return {"total": 0, "status_codes": {}, "content_types": {}}
        
        status_codes = {}
        content_types = {}
        
        for resp in self.responses:
            # 统计状态码
            status_codes[resp.status] = status_codes.get(resp.status, 0) + 1
            # 统计内容类型
            content_types[resp.content_type] = content_types.get(resp.content_type, 0) + 1
        
        return {
            "total": len(self.responses),
            "status_codes": status_codes,
            "content_types": content_types,
            "average_response_time": sum(r.response_time for r in self.responses) / len(self.responses)
        }
    
    def save_to_json(self, filename: Optional[str] = None) -> str:
        """保存为JSON文件"""
        if not filename:
            filename = f"spider_data_{self.session_id}.json"
        
        filepath = os.path.join(self.output_dir, filename)
        data = {
            "session_id": self.session_id,
            "timestamp": datetime.now().isoformat(),
            "summary": self.get_response_summary(),
            "responses": [resp.to_dict() for resp in self.responses]
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return filepath
    
    def save_to_csv(self, filename: Optional[str] = None) -> str:
        """保存为CSV文件"""
        if not filename:
            filename = f"spider_data_{self.session_id}.csv"
        
        filepath = os.path.join(self.output_dir, filename)
        
        if not self.responses:
            return filepath
        
        fieldnames = list(self.responses[0].to_dict().keys())
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for resp in self.responses:
                writer.writerow(resp.to_dict())
        
        return filepath
    
    def clear_data(self):
        """清空数据"""
        self.responses.clear()
    
    def filter_responses(self, status_code: Optional[int] = None, 
                        content_type: Optional[str] = None) -> List[ResponseData]:
        """过滤响应数据"""
        filtered = self.responses
        
        if status_code:
            filtered = [r for r in filtered if r.status == status_code]
        
        if content_type:
            filtered = [r for r in filtered if content_type in r.content_type]
        
        return filtered
