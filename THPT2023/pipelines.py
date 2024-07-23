# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sys
import os
import sys
import os
import pymysql
import redis
from sqlalchemy import create_engine, Column, String, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

BATCH_SIZE = 100
Base = declarative_base()

class Student(Base):
    __tablename__ = 'y2021'
    sbd = Column(String(10), primary_key=True)
    toan = Column(DECIMAL(10, 3))
    van = Column(DECIMAL(10, 3))
    ngoaiNgu = Column(DECIMAL(10, 3))
    vatLy = Column(DECIMAL(10, 3))
    hoaHoc = Column(DECIMAL(10, 3))
    sinhHoc = Column(DECIMAL(10, 3))
    diemTBTuNhien = Column(DECIMAL(10, 3))
    lichSu = Column(DECIMAL(10, 3))
    diaLy = Column(DECIMAL(10, 3))
    gdcd = Column(DECIMAL(10, 3))
    diemTBXaHoi = Column(DECIMAL(10, 3))

DB_USER = 'root'
DB_PASSWORD = '123456'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'thpt'

DATABASE_URI = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DATABASE_URI)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

class Thpt2023Pipeline:
    def __init__(self, batch_size=BATCH_SIZE):
        self.batch_size = batch_size
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.items_key = 'thpt2023:items'
        self.db_connection = None
        self.cursor = None

    def open_spider(self, spider):
        """Initializes database connection and cursor."""
        db_config = self.parse_database_uri(DATABASE_URI)
        self.db_connection = pymysql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            port=db_config['port']
        )
        self.cursor = self.db_connection.cursor()

    def close_spider(self, spider):
        """Inserts remaining items from Redis and closes the session."""
        if self.redis_client.llen(self.items_key) > 0:
            self.insert_batch(spider)
        self.db_connection.close()

    def process_item(self, item, spider):
        """Cache items in Redis and insert in batches."""
        self.redis_client.rpush(self.items_key, str(item))
        if self.redis_client.llen(self.items_key) >= self.batch_size:
            self.insert_batch(spider)
        return item

    def insert_batch(self, spider):
        """Retrieve batch of items from Redis and insert into the database."""
        try:
            items = []
            for _ in range(self.batch_size):
                item = self.redis_client.lpop(self.items_key)
                if item:
                    items.append(eval(item))
                else:
                    break
            
            if items:
                query = """
                    INSERT INTO students (sbd, toan, van, ngoaiNgu, vatLy, hoaHoc, sinhHoc, diemTBTuNhien, lichSu, diaLy, gdcd, diemTBXaHoi)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        toan = VALUES(toan),
                        van = VALUES(van),
                        ngoaiNgu = VALUES(ngoaiNgu),
                        vatLy = VALUES(vatLy),
                        hoaHoc = VALUES(hoaHoc),
                        sinhHoc = VALUES(sinhHoc),
                        diemTBTuNhien = VALUES(diemTBTuNhien),
                        lichSu = VALUES(lichSu),
                        diaLy = VALUES(diaLy),
                        gdcd = VALUES(gdcd),
                        diemTBXaHoi = VALUES(diemTBXaHoi)
                """
                data = [(item['sbd'], item['toan'], item['van'], item['ngoaiNgu'], item['vatLy'], item['hoaHoc'],
                         item['sinhHoc'], item['diemTBTuNhien'], item['lichSu'], item['diaLy'], item['gdcd'], item['diemTBXaHoi'])
                        for item in items]
                
                # Validate and clean the data before insertion
                cleaned_data = validate_and_clean_data(data)

                self.cursor.executemany(query, cleaned_data)
                self.db_connection.commit()
                print('Data pushed')
        except Exception as e:
            self.db_connection.rollback()
            spider.logger.error(f"Error inserting batch: {e}")

    def parse_database_uri(self, uri):
        """Parse the DATABASE_URI to extract connection parameters."""
        import re
        print(f"Parsing DATABASE_URI: {uri}")
        pattern = re.compile(r'mysql\+pymysql:\/\/(.*?):(.*?)@(.*?):(.*?)\/(.*?)$')
        match = pattern.match(uri)
        if not match:
            raise ValueError("Invalid DATABASE_URI format")
        return {
            'user': match.group(1),
            'password': match.group(2),
            'host': match.group(3),
            'port': int(match.group(4)),
            'database': match.group(5)
        }

def validate_and_clean_data(data):
    """
    Validate and clean the data before insertion.
    Convert empty strings to None for decimal columns.
    """
    cleaned_data = []
    for record in data:
        cleaned_record = tuple(
            None if value == '' else
            str(value).replace('\n', '').replace('\r', '').replace('\t', '').strip() if isinstance(value, str) else value
            for value in record
        )
        cleaned_data.append(cleaned_record)
    return cleaned_data
