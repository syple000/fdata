import unittest
import os
import tempfile
import logging
from .dao import SQLiteDAO, DatabaseConnectionError, DatabaseOperationError

class TestSQLiteDAO(unittest.TestCase):
    
    def setUp(self):
        """测试前准备"""
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.db_path = self.temp_db.name
        self.temp_db.close()
        
        # 删除数据库文件以确保测试环境干净
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        
        self.dao = SQLiteDAO(self.db_path, log_level=logging.WARNING)
    
    def tearDown(self):
        """测试后清理"""
        if self.dao.connection:
            self.dao.disconnect()
        
        # 删除临时数据库文件
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_connect_and_disconnect(self):
        """测试连接和断开数据库"""
        # 测试连接
        self.dao.connect()
        self.assertIsNotNone(self.dao.connection)
        
        # 测试断开连接
        self.dao.disconnect()
        self.assertIsNone(self.dao.connection)
    
    def test_create_table(self):
        """测试创建表"""
        self.dao.connect()
        
        # 测试创建表
        columns = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "email": "TEXT UNIQUE",
            "age": "INTEGER"
        }
        self.dao.create_table("users", columns)
        
        # 验证表是否创建成功
        cursor = self.dao.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "users")
    
    def test_create_table_without_connection(self):
        """测试未连接状态下创建表"""
        with self.assertRaises(DatabaseConnectionError):
            self.dao.create_table("test", {"id": "INTEGER"})
    
    def test_create_index(self):
        """测试创建索引"""
        self.dao.connect()
        
        # 先创建表
        self.dao.create_table("users", {
            "id": "INTEGER PRIMARY KEY",
            "email": "TEXT"
        })
        
        # 创建索引
        self.dao.create_index("idx_email", "users", ["email"], unique=True)
        
        # 验证索引是否创建成功
        cursor = self.dao.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_email'")
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "idx_email")
    
    def test_create_index_without_connection(self):
        """测试未连接状态下创建索引"""
        with self.assertRaises(DatabaseConnectionError):
            self.dao.create_index("idx_test", "test", ["id"])
    
    def test_insert(self):
        """测试插入数据"""
        self.dao.connect()
        
        # 创建表
        self.dao.create_table("users", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "email": "TEXT",
            "age": "INTEGER"
        })
        
        # 插入数据
        data = {
            "name": "张三",
            "email": "zhangsan@example.com",
            "age": 25
        }
        user_id = self.dao.insert("users", data)
        
        # 验证插入成功
        self.assertIsInstance(user_id, int)
        self.assertGreater(user_id, 0)
        
        # 验证数据确实插入
        cursor = self.dao.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE id=?", (user_id,))
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1)
    
    def test_insert_without_connection(self):
        """测试未连接状态下插入数据"""
        with self.assertRaises(DatabaseConnectionError):
            self.dao.insert("test", {"name": "test"})
    
    def test_update(self):
        """测试更新数据"""
        self.dao.connect()
        
        # 创建表并插入数据
        self.dao.create_table("users", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "age": "INTEGER"
        })
        
        user_id = self.dao.insert("users", {"name": "张三", "age": 25})
        
        # 更新数据
        affected_rows = self.dao.update("users", {"age": 26}, "id = ?", [user_id])
        
        # 验证更新成功
        self.assertEqual(affected_rows, 1)
        
        # 验证数据确实更新
        cursor = self.dao.connection.cursor()
        cursor.execute("SELECT age FROM users WHERE id=?", (user_id,))
        age = cursor.fetchone()[0]
        self.assertEqual(age, 26)
    
    def test_update_without_connection(self):
        """测试未连接状态下更新数据"""
        with self.assertRaises(DatabaseConnectionError):
            self.dao.update("test", {"name": "test"}, "id=1")
    
    def test_delete(self):
        """测试删除数据"""
        self.dao.connect()
        
        # 创建表并插入数据
        self.dao.create_table("users", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "age": "INTEGER"
        })
        
        user_id = self.dao.insert("users", {"name": "张三", "age": 25})
        
        # 删除数据
        affected_rows = self.dao.delete("users", "id = ?", [user_id])
        
        # 验证删除成功
        self.assertEqual(affected_rows, 1)
        
        # 验证数据确实删除
        cursor = self.dao.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE id=?", (user_id,))
        count = cursor.fetchone()[0]
        self.assertEqual(count, 0)
    
    def test_delete_without_connection(self):
        """测试未连接状态下删除数据"""
        with self.assertRaises(DatabaseConnectionError):
            self.dao.delete("test", "id=1")
    
    def test_select(self):
        """测试查询数据"""
        self.dao.connect()
        
        # 创建表并插入数据
        self.dao.create_table("users", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "email": "TEXT",
            "age": "INTEGER"
        })
        
        # 插入多条数据
        data_list = [
            {"name": "张三", "email": "zhangsan@example.com", "age": 25},
            {"name": "李四", "email": "lisi@example.com", "age": 30},
            {"name": "王五", "email": "wangwu@example.com", "age": 20}
        ]
        
        for data in data_list:
            self.dao.insert("users", data)
        
        # 测试查询所有数据
        all_users = self.dao.select("users")
        self.assertEqual(len(all_users), 3)
        
        # 测试带条件查询
        users_over_25 = self.dao.select("users", where_clause="age >= ?", where_params=[25])
        self.assertEqual(len(users_over_25), 2)
        
        # 测试查询特定列
        names = self.dao.select("users", columns=["name"], where_clause="age > ?", where_params=[20])
        self.assertEqual(len(names), 2)
        self.assertTrue(all("name" in user for user in names))
        
        # 测试带排序和限制
        limited_users = self.dao.select("users", order_by="age ASC", limit=2)
        self.assertEqual(len(limited_users), 2)
        self.assertEqual(limited_users[0]["age"], 20)
        self.assertEqual(limited_users[1]["age"], 25)
    
    def test_select_without_connection(self):
        """测试未连接状态下查询数据"""
        with self.assertRaises(DatabaseConnectionError):
            self.dao.select("test")
    
    def test_select_one(self):
        """测试查询单条数据"""
        self.dao.connect()
        
        # 创建表并插入数据
        self.dao.create_table("users", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "age": "INTEGER"
        })
        
        user_id = self.dao.insert("users", {"name": "张三", "age": 25})
        
        # 测试查询存在的数据
        user = self.dao.select_one("users", where_clause="id = ?", where_params=[user_id])
        self.assertIsNotNone(user)
        self.assertEqual(user["name"], "张三")
        self.assertEqual(user["age"], 25)
        
        # 测试查询不存在的数据
        non_existent = self.dao.select_one("users", where_clause="id = ?", where_params=[999])
        self.assertIsNone(non_existent)
    
    def test_execute_raw_sql(self):
        """测试执行原始SQL"""
        self.dao.connect()
        
        # 创建表
        self.dao.create_table("users", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "age": "INTEGER"
        })
        
        # 测试插入SQL
        affected_rows = self.dao.execute_raw_sql(
            "INSERT INTO users (name, age) VALUES (?, ?)",
            ["张三", 25]
        )
        self.assertEqual(affected_rows, 1)
        
        # 测试查询SQL
        results = self.dao.execute_raw_sql("SELECT * FROM users WHERE age > ?", [20])
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "张三")
    
    def test_execute_raw_sql_without_connection(self):
        """测试未连接状态下执行原始SQL"""
        with self.assertRaises(DatabaseConnectionError):
            self.dao.execute_raw_sql("SELECT 1")
    
    def test_context_manager(self):
        """测试上下文管理器"""
        with SQLiteDAO(self.db_path) as dao:
            self.assertIsNotNone(dao.connection)
            
            # 在上下文中进行数据库操作
            dao.create_table("test", {"id": "INTEGER PRIMARY KEY", "name": "TEXT"})
            dao.insert("test", {"name": "测试"})
            results = dao.select("test")
            self.assertEqual(len(results), 1)
        
        # 上下文退出后连接应该关闭
        self.assertIsNone(dao.connection)
    
    def test_comprehensive_workflow(self):
        """测试完整的数据库操作工作流"""
        with SQLiteDAO(self.db_path) as dao:
            # 创建表
            dao.create_table("users", {
                "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
                "name": "TEXT NOT NULL",
                "email": "TEXT UNIQUE",
                "age": "INTEGER"
            })
            
            # 创建索引
            dao.create_index("idx_email", "users", ["email"], unique=True)
            
            # 插入数据
            user_id = dao.insert("users", {
                "name": "张三",
                "email": "zhangsan@example.com",
                "age": 25
            })
            self.assertGreater(user_id, 0)
            
            # 查询数据
            users = dao.select("users", where_clause="age > ?", where_params=[20])
            self.assertEqual(len(users), 1)
            self.assertEqual(users[0]["name"], "张三")
            self.assertEqual(users[0]["email"], "zhangsan@example.com")
            self.assertEqual(users[0]["age"], 25)
            
            # 更新数据
            affected_rows = dao.update("users", {"age": 26}, "id = ?", [user_id])
            self.assertEqual(affected_rows, 1)
            
            # 验证更新
            updated_users = dao.select("users", where_clause="age > ?", where_params=[20])
            self.assertEqual(len(updated_users), 1)
            self.assertEqual(updated_users[0]["age"], 26)
            
            # 删除数据
            affected_rows = dao.delete("users", "age < ?", [27])
            self.assertEqual(affected_rows, 1)
            
            # 验证删除
            remaining_users = dao.select("users", where_clause="age > ?", where_params=[20])
            self.assertEqual(len(remaining_users), 0)


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 运行测试
    unittest.main(verbosity=2)