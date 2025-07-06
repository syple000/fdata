import mmap
import os
import csv
import io
from typing import List, Optional, Any


class CSVStreamDAO:
    """基于mmap的流式CSV数据存储和读取"""
    
    def __init__(self, filepath: str, mode: str = 'r+'):
        """
        初始化CSV流式DAO
        
        Args:
            filepath: CSV文件路径
            mode: 打开模式 ('r+', 'w+', 'a+')
        """
        self.filepath = filepath
        self.mode = mode
        self._file = None
        self._mmap = None
        self._read_offset = 0
        self._write_offset = 0
        self._delimiter = ','
        
        self._init_file()
    
    def _init_file(self):
        """初始化文件和mmap"""
        # 确保文件存在
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w') as f:
                pass
        
        # 打开文件
        self._file = open(self.filepath, self.mode)
        
        # 获取文件大小
        file_size = os.path.getsize(self.filepath)
        
        if file_size > 0:
            # 文件不为空才创建mmap
            self._mmap = mmap.mmap(self._file.fileno(), 0)
            self._write_offset = file_size
        else:
            # 空文件，先写入一个字节再创建mmap
            self._file.write(' ')
            self._file.flush()
            self._mmap = mmap.mmap(self._file.fileno(), 0)
            self._write_offset = 0
    
    def write_row(self, row: List[Any]) -> None:
        """
        写入单行数据
        
        Args:
            row: 要写入的行数据列表
        """
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
            self._file.write('\x00' * needed_size)  # 激进扩容，按照当前需要的体积长度扩容
            self._file.flush()
            self._mmap = mmap.mmap(self._file.fileno(), 0)
        
        # 写入数据
        self._mmap.seek(self._write_offset)
        self._mmap.write(data)
        self._write_offset += len(data)
        
        # 确保数据同步到磁盘
        self._mmap.flush()
    
    def read_row(self) -> Optional[List[str]]:
        """
        读取单行数据
        
        Returns:
            读取的行数据列表，如果到达文件末尾返回None
        """
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
            line_str = line_data.decode('utf-8').rstrip('\r\n')
            if not line_str:
                return self.read_row()  # 跳过空行
            
            reader = csv.reader([line_str], delimiter=self._delimiter)
            return next(reader)
        except (UnicodeDecodeError, csv.Error):
            # 跳过损坏的行
            return self.read_row()
    
    def reset_read_offset(self) -> None:
        """重置读取偏移量到文件开头"""
        self._read_offset = 0
    
    def reset_write_offset(self) -> None:
        """重置写入偏移量到文件开头（清空文件）"""
        self._write_offset = 0
        if self._mmap:
            self._mmap.seek(0)
            self._mmap.write(b'\x00' * len(self._mmap))
            self._mmap.flush()
    
    def get_read_offset(self) -> int:
        """获取当前读取偏移量"""
        return self._read_offset
    
    def get_write_offset(self) -> int:
        """获取当前写入偏移量"""
        return self._write_offset
    
    def set_delimiter(self, delimiter: str) -> None:
        """设置CSV分隔符"""
        self._delimiter = delimiter
    
    def close(self) -> None:
        """关闭文件和mmap"""
        if self._mmap:
            self._mmap.close()
            self._mmap = None
        
        if self._file:
            self._file.close()
            self._file = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 使用示例
if __name__ == "__main__":
    # 写入示例
    with CSVStreamDAO('/home/syple/code/fdata/test.csv', 'w+') as dao:
        dao.write_row(['name', 'age', 'city'])
        dao.write_row(['Alice', '25', 'New York'])
        dao.write_row(['Bob', '30', 'London'])
        
        # 重置读取偏移量
        dao.reset_read_offset()
        
        # 读取所有行
        while True:
            row = dao.read_row()
            if row is None:
                break
            print(row)