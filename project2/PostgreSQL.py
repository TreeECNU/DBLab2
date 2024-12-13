import sqlite3
import psycopg2
from psycopg2 import sql

# 连接到SQLite数据库
sqlite_conn = sqlite3.connect('bookstore/fe/data/book.db')
sqlite_cursor = sqlite_conn.cursor()


# 连接到PostgreSQL数据库
postgres_conn = psycopg2.connect(
    dbname='bookstore',  # 这里需要在postgreSQL提前创建好bookstore数据库
    user='postgres',  # 超级用户名
    password='1527088306',  # 密码
    host='localhost',  # 相应数据库的地址
    port='5432'  # PostgreSQL默认端口
)
postgres_cursor = postgres_conn.cursor()

# 创建表（如果不存在）
postgres_cursor.execute("""
CREATE TABLE IF NOT EXISTS books (
    id SERIAL PRIMARY KEY,
    title TEXT,
    author TEXT,
    publisher TEXT,
    original_title TEXT,
    translator TEXT,
    pub_year TEXT,
    pages INTEGER,
    price REAL,
    currency_unit TEXT,
    binding TEXT,
    isbn TEXT,
    author_intro TEXT,
    book_intro TEXT,
    content TEXT,
    tags TEXT,
    picture BYTEA
);
""")
postgres_conn.commit()

# 从SQLite中查询book表的所有记录
sqlite_cursor.execute("SELECT * FROM book")
rows = sqlite_cursor.fetchall()

# 遍历每一行并插入到PostgreSQL中
for row in rows:
    insert_query = sql.SQL("""
    INSERT INTO books (id, title, author, publisher, original_title, translator, 
                       pub_year, pages, price, currency_unit, binding, isbn, 
                       author_intro, book_intro, content, tags, picture)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    
    # 插入到PostgreSQL中
    postgres_cursor.execute(insert_query, row)

# 提交事务
postgres_conn.commit()

# 关闭数据库连接
sqlite_conn.close()
postgres_conn.close()

print("数据迁移完成！")