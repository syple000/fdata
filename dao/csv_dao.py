import mmap
import os
import csv
import io
from typing import List, Optional, Any, Type, TypeVar, Generic, get_type_hints
from dataclasses import dataclass, fields
import json

T = TypeVar('T')

class CSVGenericDAO(Generic[T]):
    """基于mmap的泛型CSV数据存储和读取"""
    
    def __init__(self, filepath: str, model_class: Type[T]):
        """
        初始化CSV泛型DAO
        
        Args:
            filepath: CSV文件路径
            model_class: 数据模型类（必须是dataclass）
        """
        self.filepath = filepath
        self.model_class = model_class
        self._file = None
        self._mmap = None
        self._read_offset = 0
        self._write_offset = 0
        self._delimiter = ','
        self._headers = []
        
        # 验证模型类
        if not hasattr(model_class, '__dataclass_fields__'):
            raise ValueError(f"{model_class.__name__} must be a dataclass")
        
        # 获取字段名作为列名
        self._headers = [field.name for field in fields(model_class)]
        
        self._init_file()
    
    def _init_file(self):
        """初始化文件和mmap"""
        file_exists = os.path.exists(self.filepath)
        if not file_exists or os.path.getsize(self.filepath) == 0:
            # 创建新文件并写入头部
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=self._delimiter)
                writer.writerow(self._headers)
        
        self._file = open(self.filepath, 'r+', encoding='utf-8')
        
        file_size = os.path.getsize(self.filepath)
        
        self._mmap = mmap.mmap(self._file.fileno(), 0)
        self._write_offset = file_size
        
        self._validate_headers()
        self._skip_headers()
    
    def _validate_headers(self):
        """验证文件头部是否匹配"""
        self._mmap.seek(0)
        first_line = self._read_line_from_mmap()
        if first_line:
            try:
                reader = csv.reader([first_line], delimiter=self._delimiter)
                file_headers = next(reader)
                if file_headers != self._headers:
                    raise ValueError(f"File headers {file_headers} don't match model fields {self._headers}")
            except csv.Error:
                raise ValueError("Invalid CSV header format")
        else:
            raise ValueError("File is empty or invalid")
    
    def _skip_headers(self):
        """跳过头部，设置读取偏移量"""
        self._mmap.seek(0)
        first_line = self._read_line_from_mmap()
        if first_line:
            self._read_offset = len(first_line.encode('utf-8'))
        else:
            raise Exception("File is empty or invalid")
    
    def _read_line_from_mmap(self) -> Optional[str]:
        """从mmap读取一行"""
        line_data = b''
        
        while self._mmap.tell() < len(self._mmap):
            char = self._mmap.read(1)
            line_data += char
            
            if char == b'\n':
                break
        
        if not line_data:
            return None
        
        try:
            return line_data.decode('utf-8').rstrip('\r\n')
        except UnicodeDecodeError:
            return None
    
    def write_record(self, record: T) -> None:
        """
        写入单条记录
        
        Args:
            record: 要写入的记录对象
        """
        if not isinstance(record, self.model_class):
            raise TypeError(f"Record must be instance of {self.model_class.__name__}")
        
        # 提取字段值
        row_data = []
        for field in fields(self.model_class):
            value = getattr(record, field.name)
            # 处理复杂类型，转换为字符串
            if isinstance(value, (list, dict)):
                row_data.append(json.dumps(value, ensure_ascii=False))
            else:
                row_data.append(str(value) if value is not None else '')
        
        self._write_row(row_data)
    
    def write_records(self, records: List[T]) -> None:
        """
        写入多条记录
        
        Args:
            records: 要写入的记录对象列表
        """
        for record in records:
            self.write_record(record)
    
    def _write_row(self, row: List[str]) -> None:
        """写入单行数据"""
        if not self._mmap:
            return
        
        # 转换为CSV格式字符串
        output = io.StringIO()
        writer = csv.writer(output, delimiter=self._delimiter)
        writer.writerow(row)
        csv_line = output.getvalue()
        
        # 编码为字节
        data = csv_line.encode('utf-8')
        
        # 检查是否需要扩展文件
        current_size = len(self._mmap)
        needed_size = self._write_offset + len(data)
        
        if needed_size > current_size:
            # 扩展文件大小
            self._mmap.close()
            self._file.seek(0, 2)  # 移动到文件末尾
            self._file.write('\x00' * max(needed_size - current_size, 1024*1024))
            self._file.flush()
            self._mmap = mmap.mmap(self._file.fileno(), 0)
        
        # 写入数据
        self._mmap.seek(self._write_offset)
        self._mmap.write(data)
        self._write_offset += len(data)
        
        # 确保数据同步到磁盘
        # self._mmap.flush()
    
    def read_record(self) -> Optional[T]:
        """
        读取单条记录
        
        Returns:
            读取的记录对象，如果到达文件末尾返回None
        """
        row = self._read_row()
        if row is None:
            return None
        
        return self._row_to_record(row)
    
    def read_records(self, limit: Optional[int] = None) -> List[T]:
        """
        读取多条记录
        
        Args:
            limit: 最大读取数量，None表示读取所有
            
        Returns:
            记录对象列表
        """
        records = []
        count = 0
        
        while True:
            if limit is not None and count >= limit:
                break
                
            record = self.read_record()
            if record is None:
                break
                
            records.append(record)
            count += 1
        
        return records
    
    def _read_row(self) -> Optional[List[str]]:
        """读取单行数据"""
        if not self._mmap or self._read_offset >= self._write_offset:
            return None
        
        # 从当前读取位置开始查找行结束符
        self._mmap.seek(self._read_offset)
        
        # 读取到行结束符或文件末尾
        line_data = b''
        while self._read_offset < self._write_offset:
            char = self._mmap.read(1)
            self._read_offset += 1
            line_data += char
            
            if char == b'\n':
                break
        
        if not line_data:
            return None
        
        # 解码并解析CSV
        try:
            line_str = line_data.decode('utf-8').rstrip('\x00\r\n')
            if not line_str:
                return self._read_row()  # 跳过空行
            
            reader = csv.reader([line_str], delimiter=self._delimiter)
            return next(reader)
        except (UnicodeDecodeError, csv.Error):
            # 跳过损坏的行
            return self._read_row()
    
    def _row_to_record(self, row: List[str]) -> T:
        """将行数据转换为记录对象"""
        if len(row) != len(self._headers):
            raise ValueError(f"Row length {len(row)} doesn't match headers length {len(self._headers)}")
        
        # 获取类型提示
        type_hints = get_type_hints(self.model_class)
        
        # 构建字段值字典
        field_values = {}
        for i, field in enumerate(fields(self.model_class)):
            field_name = field.name
            field_type = type_hints.get(field_name, str)
            raw_value = row[i] if i < len(row) else ''
            
            # 类型转换
            if raw_value == '':
                field_values[field_name] = None
            else:
                field_values[field_name] = self._convert_value(raw_value, field_type)
        
        return self.model_class(**field_values)
    
    def _convert_value(self, value: str, target_type: Type) -> Any:
        """值类型转换"""
        if target_type == str:
            return value
        elif target_type == int:
            return int(value)
        elif target_type == float:
            return float(value)
        elif target_type == bool:
            return value.lower() in ('true', '1', 'yes', 'on')
        elif target_type == list:
            return json.loads(value)
        elif target_type == dict:
            return json.loads(value)
        else:
            # 尝试直接构造
            try:
                return target_type(value)
            except:
                return value
    
    def reset_read_offset(self) -> None:
        """重置读取偏移量到头部后"""
        self._read_offset = 0
        self._skip_headers()
    
    def close(self) -> None:
        """关闭文件和mmap"""
        if self._mmap:
            self._mmap.close()
            self._mmap = None
        
        if self._file:
            self._file.truncate(self._write_offset)
            self._file.close()
            self._file = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()



# 使用示例
@dataclass
class Person:
    name: str
    age: int
    city: str
    hobbies: List[str]


@dataclass
class Product:
    id: int
    name: str
    price: float
    in_stock: bool


if __name__ == "__main__":
    os.remove('/tmp/people.csv') if os.path.exists('/tmp/people.csv') else None
    os.remove('/tmp/products.csv') if os.path.exists('/tmp/products.csv') else None

    # 使用Person模型
    with CSVGenericDAO('/tmp/people.csv', Person) as dao:
        # 写入单条记录
        person1 = Person(name="Alice", age=25, city="New York", hobbies=["reading", "swimming"])
        dao.write_record(person1)
        
        # 写入多条记录
        people = [
            Person(name="Bob", age=30, city="London", hobbies=["coding", "gaming"]),
            Person(name="Charlie", age=35, city="Paris", hobbies=["cooking", "traveling"])
        ]
        dao.write_records(people)
        
        # 重置读取偏移量
        dao.reset_read_offset()
        
        # 读取所有记录
        all_people = dao.read_records()
        for person in all_people:
            print(f"{person.name}, {person.age}, {person.city}, {person.hobbies}")
    
    with CSVGenericDAO('/tmp/people.csv', Person) as dao:
        # 读取单条记录
        while True:
            person = dao.read_record()
            if person is None:
                break
            print(f"Person: {person.name}, Age: {person.age}, City: {person.city}, Hobbies: {person.hobbies}")
    
    # 使用Product模型
    with CSVGenericDAO('/tmp/products.csv', Product) as dao:
        products = [
            Product(id=1, name="Laptop", price=999.99, in_stock=True),
            Product(id=2, name="Mouse", price=29.99, in_stock=False)
        ]
        dao.write_records(products)
        
        dao.reset_read_offset()
        
        # 读取单条记录
        while True:
            product = dao.read_record()
            if product is None:
                break
            print(f"Product: {product.name}, Price: ${product.price}, In Stock: {product.in_stock}")
    
    with CSVGenericDAO('/tmp/products.csv', Product) as dao:
        # 读取所有记录
        all_products = dao.read_records()
        for product in all_products:
            print(f"{product.id}, {product.name}, ${product.price}, In Stock: {product.in_stock}")