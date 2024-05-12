# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import scrapy
import pymongo
import json
from bson.objectid import ObjectId
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import mysql.connector
import psycopg2


class JsonDBBookPipeline:
    def open_spider(self, spider):
        self.file = open('jsondatabook.json','a',encoding='utf-8')

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.file.write(line)
        return item
    
    def close_spider(self, spider):
        self.file.close()

class CSVDBBookPipeline:
    def process_item(self, item, spider):
        self.file = open('bookdataname.csv','a', encoding='utf-8')
        line = ''
        # line += 'coursename$lecturer$intro$describe$courseUrl$votenumber$rating$newfee$oldfee$lessonnum' + '\n'
        line += item["bookname"]
        line += ',' + item["cost"] 
        line += ',' + item["stock"] 
        line += ',' + item["description"] 
        line += '\n'
        print('line: ', line)
        self.file.write(line)
        self.file.close
        return item
    
    
class MongoDBBookPipeline:
    def __init__(self):
        '''
        self.client = pymongo.MongoClient('mongodb+srv://........./?retryWrites=true&w=majority&appName=.....')
        self.db = self.client['lawnet']
        '''
        
        self.client = pymongo.MongoClient('mongodb+srv://117611xuan:117611xuan@cluster0.xqekflr.mongodb.net/')
        self.db = self.client['Book']
    
    def process_item(self, item, spider):
        collection =self.db['book']
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")
        pass


class MySQLBookPipeline:
    # Tham khảo: https://scrapeops.io/python-scrapy-playbook/scrapy-save-data-mysql/
    def __init__(self):
        # Thông tin kết nối csdl
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = '',
            database = 'bookscrapy'
            
            # host = 'mysqlscrapy',
            # user = 'admin',
            # password = 'admin',
            # database = 'bookscrapy'
        )

        # Tạo con trỏ để thực thi các câu lệnh
        self.cur = self.conn.cursor()

        # Tạo bảng chứa dữ liệu nếu không tồn tại
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS bookscrapy (
                id INT NOT NULL auto_increment,
                bookname VARCHAR(255),
                cost VARCHAR(255),
                stock TEXT,
                description TEXT,
                BookUrl VARCHAR(255),
                PRIMARY KEY (id)            
            )
        """)

    def process_item(self, item, spider):
        # Kiểm tra xem khoá học đã tồn tại chưa
        self.cur.execute("SELECT * FROM bookscrapy WHERE bookname = %s", (str(item['bookname']),))
        result = self.cur.fetchone()

        ## Hiện thông báo nếu đã tồn tại trong csdl
        if result:
            spider.logger.warn("Item đã có trong csdl MySQL: %s" % item['bookname'])


        ## Thêm dữ liệu nếu chưa tồn tại
        else:
            # Định nghĩa cách thức thêm dữ liệu
            self.cur.execute(""" INSERT INTO bookscrapy(bookname, cost, stock, description, BookUrl) 
                                VALUES (%s, %s, %s, %s, %s)""", (
                                item["bookname"], 
                                str(item["cost"]), 
                                str(item["stock"]), 
                                str(item["description"]), 
                                str(item["BookUrl"]), ))

            # Thực hiện insert dữ liệu vào csdl
            self.conn.commit()
        return item

    def close_connect(self, spider):
        # Đóng kết nối csdl
        self.cur.close()
        self.conn.close()

# #POSTGRES
# class PostgresBookPipeline:
#     def __init__(self):
#         # Kết nối đến cơ sở dữ liệu PostgreSQL trên máy chủ "neon"
#         self.connection = psycopg2.connect(
#             dbname='Book',    # Tên cơ sở dữ liệu
#             user='neondb_owner',  # Tên người dùng PostgreSQL
#             password='TmQlM5Weabt0',  # Mật khẩu
#             host='ep-restless-smoke-a51hhvg8.us-east-2.aws.neon.tech',  # Địa chỉ host
#             sslmode='require'   # Yêu cầu sử dụng SSL
#         )

#     def process_item(self, item, spider):
#         # Lưu item vào cơ sở dữ liệu PostgreSQL
#         try:
#             with self.connection.cursor() as cursor:
#                 sql = "INSERT INTO bookscrapy (bookname, cost, stock, description, BookUrl) VALUES (%s, %s, %s, %s, %s)"
#                 cursor.execute(sql, (
#                     item.get('bookname', ''),
#                     item.get('cost', ''),
#                     item.get('stock', ''),
#                     item.get('description', ''),
#                     item.get('BookUrl', ''),
#                 ))
#             self.connection.commit()
#             return item
#         except Exception as e:
#             raise DropItem(f"Error inserting item into PostgreSQL: {e}")

        
# postres
class PostgresBookPipeline:
    def __init__(self):
        hostname = 'localhost'
        username = 'postgres'
        password = 'admin'
        database = 'bookscrapy'
        
        # hostname = 'postgresscrapy'
        # username = 'admin'
        # password = 'admin'
        # database = 'bookscrapy'

        self.conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.conn.cursor()

        self.cur.execute("""
                         CREATE TABLE IF NOT EXISTS bookscrapy (
                            id SERIAL PRIMARY KEY,
                            bookname VARCHAR(255),
                            cost VARCHAR(255),
                            stock TEXT,
                            description TEXT,
                            BookUrl VARCHAR(255)
                        );
                        """)

    def process_item(self, item, spider):
        self.cur.execute("SELECT * FROM bookscrapy WHERE bookname = %s", (str(item["bookname"]),))
        result = self.cur.fetchone()

        if result: 
            spider.logger.warn("Khoá học đã tồn tại trên csdl: %s" % item["bookname"])

        else:
            self.cur.execute(""" INSERT INTO bookscrapy(bookname, cost, stock, description, BookUrl) 
                                    VALUES (%s, %s, %s, %s, %s)""", (
                                    str(item["bookname"]), 
                                    str(item["cost"]), 
                                    str(item["stock"]), 
                                    str(item["description"]), 
                                    str(item["BookUrl"]), ))
            
            self.conn.commit()
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
