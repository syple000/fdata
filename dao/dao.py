import sqlite3
import logging
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

class DatabaseError(Exception):
    """数据库操作异常基类"""
    pass

class DatabaseConnectionError(DatabaseError):
    """数据库连接异常"""
    pass

class DatabaseOperationError(DatabaseError):
    """数据库操作异常"""
    pass

class SQLiteDAO:
    """SQLite 数据库访问对象，提供基础的数据库操作功能"""
    
    def __init__(self, db_path: str, log_level: int = logging.INFO):
        """
        初始化 DAO
        
        Args:
            db_path: 数据库文件路径
            log_level: 日志级别
        """
        self.db_path = db_path
        self.connection = None
        
        # 获取模块logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        
        # 确保数据库目录存在
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def connect(self):
        """
        连接到数据库
        
        Raises:
            DatabaseConnectionError: 连接失败时抛出
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # 返回字典格式的行
            self.logger.info(f"成功连接到数据库: {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"数据库连接失败: {e}")
            raise DatabaseConnectionError(f"数据库连接失败: {e}")
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("数据库连接已关闭")
    
    def create_table(self, table_name: str, columns: Dict[str, str]):
        """
        创建表
        
        Args:
            table_name: 表名
            columns: 列定义字典 {'column_name': 'column_type'}
            
        Raises:
            DatabaseConnectionError: 数据库未连接时抛出
            DatabaseOperationError: 创建表失败时抛出
        """
        if not self.connection:
            self.logger.error("数据库未连接")
            raise DatabaseConnectionError("数据库未连接")
        
        try:
            column_definitions = []
            for col_name, col_type in columns.items():
                column_definitions.append(f"{col_name} {col_type}")
            
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
            
            cursor = self.connection.cursor()
            cursor.execute(sql)
            self.connection.commit()
            
            self.logger.info(f"表 '{table_name}' 创建成功")
        except sqlite3.Error as e:
            self.logger.error(f"创建表 '{table_name}' 失败: {e}")
            raise DatabaseOperationError(f"创建表 '{table_name}' 失败: {e}")
    
    def create_index(self, index_name: str, table_name: str, columns: List[str], unique: bool = False):
        """
        创建索引
        
        Args:
            index_name: 索引名
            table_name: 表名
            columns: 索引列列表
            unique: 是否为唯一索引
            
        Raises:
            DatabaseConnectionError: 数据库未连接时抛出
            DatabaseOperationError: 创建索引失败时抛出
        """
        if not self.connection:
            self.logger.error("数据库未连接")
            raise DatabaseConnectionError("数据库未连接")
        
        try:
            unique_keyword = "UNIQUE" if unique else ""
            columns_str = ", ".join(columns)
            sql = f"CREATE {unique_keyword} INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns_str})"
            
            cursor = self.connection.cursor()
            cursor.execute(sql)
            self.connection.commit()
            
            self.logger.info(f"索引 '{index_name}' 在表 '{table_name}' 上创建成功")
        except sqlite3.Error as e:
            self.logger.error(f"创建索引 '{index_name}' 失败: {e}")
            raise DatabaseOperationError(f"创建索引 '{index_name}' 失败: {e}")
    
    def insert(self, table_name: str, data: Dict[str, Any]) -> int:
        """
        插入数据
        
        Args:
            table_name: 表名
            data: 要插入的数据字典
            
        Returns:
            int: 插入行的 ID
            
        Raises:
            DatabaseConnectionError: 数据库未连接时抛出
            DatabaseOperationError: 插入数据失败时抛出
        """
        if not self.connection:
            self.logger.error("数据库未连接")
            raise DatabaseConnectionError("数据库未连接")
        
        try:
            columns = list(data.keys())
            placeholders = ["?" for _ in columns]
            values = list(data.values())
            
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            
            cursor = self.connection.cursor()
            cursor.execute(sql, values)
            self.connection.commit()
            
            row_id = cursor.lastrowid
            self.logger.info(f"数据插入成功，表: {table_name}, ID: {row_id}")
            return row_id
        except sqlite3.Error as e:
            self.logger.error(f"插入数据失败，表: {table_name}, 错误: {e}")
            raise DatabaseOperationError(f"插入数据失败，表: {table_name}, 错误: {e}")
    
    def update(self, table_name: str, data: Dict[str, Any], where_clause: str, where_params: List[Any] = None) -> int:
        """
        更新数据
        
        Args:
            table_name: 表名
            data: 要更新的数据字典
            where_clause: WHERE 子句
            where_params: WHERE 子句的参数
            
        Returns:
            int: 受影响的行数
            
        Raises:
            DatabaseConnectionError: 数据库未连接时抛出
            DatabaseOperationError: 更新数据失败时抛出
        """
        if not self.connection:
            self.logger.error("数据库未连接")
            raise DatabaseConnectionError("数据库未连接")
        
        try:
            set_clauses = []
            values = []
            
            for column, value in data.items():
                set_clauses.append(f"{column} = ?")
                values.append(value)
            
            sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {where_clause}"
            
            if where_params:
                values.extend(where_params)
            
            cursor = self.connection.cursor()
            cursor.execute(sql, values)
            self.connection.commit()
            
            affected_rows = cursor.rowcount
            self.logger.info(f"数据更新成功，表: {table_name}, 受影响行数: {affected_rows}")
            return affected_rows
        except sqlite3.Error as e:
            self.logger.error(f"更新数据失败，表: {table_name}, 错误: {e}")
            raise DatabaseOperationError(f"更新数据失败，表: {table_name}, 错误: {e}")
    
    def delete(self, table_name: str, where_clause: str, where_params: List[Any] = None) -> int:
        """
        删除数据
        
        Args:
            table_name: 表名
            where_clause: WHERE 子句
            where_params: WHERE 子句的参数
            
        Returns:
            int: 受影响的行数
            
        Raises:
            DatabaseConnectionError: 数据库未连接时抛出
            DatabaseOperationError: 删除数据失败时抛出
        """
        if not self.connection:
            self.logger.error("数据库未连接")
            raise DatabaseConnectionError("数据库未连接")
        
        try:
            sql = f"DELETE FROM {table_name} WHERE {where_clause}"
            
            cursor = self.connection.cursor()
            cursor.execute(sql, where_params or [])
            self.connection.commit()
            
            affected_rows = cursor.rowcount
            self.logger.info(f"数据删除成功，表: {table_name}, 受影响行数: {affected_rows}")
            return affected_rows
        except sqlite3.Error as e:
            self.logger.error(f"删除数据失败，表: {table_name}, 错误: {e}")
            raise DatabaseOperationError(f"删除数据失败，表: {table_name}, 错误: {e}")
    
    def select(self, table_name: str, columns: List[str] = None, where_clause: str = None, 
               where_params: List[Any] = None, order_by: str = None, limit: int = None) -> List[Dict[str, Any]]:
        """
        查询数据
        
        Args:
            table_name: 表名
            columns: 要查询的列，None 表示所有列
            where_clause: WHERE 子句
            where_params: WHERE 子句的参数
            order_by: ORDER BY 子句
            limit: LIMIT 限制
            
        Returns:
            List[Dict[str, Any]]: 查询结果列表
            
        Raises:
            DatabaseConnectionError: 数据库未连接时抛出
            DatabaseOperationError: 查询数据失败时抛出
        """
        if not self.connection:
            self.logger.error("数据库未连接")
            raise DatabaseConnectionError("数据库未连接")
        
        try:
            columns_str = ", ".join(columns) if columns else "*"
            sql = f"SELECT {columns_str} FROM {table_name}"
            
            if where_clause:
                sql += f" WHERE {where_clause}"
            
            if order_by:
                sql += f" ORDER BY {order_by}"
            
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor = self.connection.cursor()
            cursor.execute(sql, where_params or [])
            
            rows = cursor.fetchall()
            result = [dict(row) for row in rows]
            
            self.logger.info(f"数据查询成功，表: {table_name}, 返回 {len(result)} 行")
            return result
        except sqlite3.Error as e:
            self.logger.error(f"查询数据失败，表: {table_name}, 错误: {e}")
            raise DatabaseOperationError(f"查询数据失败，表: {table_name}, 错误: {e}")
    
    def select_one(self, table_name: str, columns: List[str] = None, where_clause: str = None, 
                   where_params: List[Any] = None) -> Optional[Dict[str, Any]]:
        """
        查询单条数据
        
        Args:
            table_name: 表名
            columns: 要查询的列，None 表示所有列
            where_clause: WHERE 子句
            where_params: WHERE 子句的参数
            
        Returns:
            Optional[Dict[str, Any]]: 查询结果，未找到返回 None
            
        Raises:
            DatabaseConnectionError: 数据库未连接时抛出
            DatabaseOperationError: 查询数据失败时抛出
        """
        results = self.select(table_name, columns, where_clause, where_params, limit=1)
        return results[0] if results else None
    
    def execute_raw_sql(self, sql: str, params: List[Any] = None) -> Union[List[Dict[str, Any]], int]:
        """
        执行原始 SQL 语句
        
        Args:
            sql: SQL 语句
            params: SQL 参数
            
        Returns:
            Union[List[Dict[str, Any]], int]: SELECT 返回结果列表，其他返回受影响行数
            
        Raises:
            DatabaseConnectionError: 数据库未连接时抛出
            DatabaseOperationError: 执行SQL失败时抛出
        """
        if not self.connection:
            self.logger.error("数据库未连接")
            raise DatabaseConnectionError("数据库未连接")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, params or [])
            
            if sql.strip().upper().startswith("SELECT"):
                rows = cursor.fetchall()
                result = [dict(row) for row in rows]
                self.logger.info(f"原始 SQL 查询成功，返回 {len(result)} 行")
                return result
            else:
                self.connection.commit()
                affected_rows = cursor.rowcount
                self.logger.info(f"原始 SQL 执行成功，受影响行数: {affected_rows}")
                return affected_rows
        except sqlite3.Error as e:
            self.logger.error(f"执行原始 SQL 失败: {e}")
            raise DatabaseOperationError(f"执行原始 SQL 失败: {e}")
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()

if __name__ == "__main__":
    # 新建账户数据库（请注意不要遗忘密码）：账户名、类型、加密私钥、盐值、地址、余额
    db_path = "database/accounts.db"
    with SQLiteDAO(db_path) as dao:
        dao.create_table("accounts", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "type": "TEXT NOT NULL",
            "encrypted_private_key": "TEXT NOT NULL UNIQUE",
            "salt": "TEXT NOT NULL",
            "address": "TEXT NOT NULL UNIQUE",
            "balance": "REAL DEFAULT 0.0"
        })
        dao.create_index("idx_accounts_name", "accounts", ["name"], unique=False)