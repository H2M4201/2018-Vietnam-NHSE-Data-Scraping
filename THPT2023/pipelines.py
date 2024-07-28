# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sys
import os
import pymysql
from sqlalchemy import create_engine, Column, String, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

BATCH_SIZE = 1000
Base = declarative_base()

class Student(Base):
    __tablename__ = 'y2020'
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
        self.buffer = []
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
        if self.buffer:
            self.insert_batch_from_buffer(spider)
        self.db_connection.close()

    def process_item(self, item, spider):
        self.buffer.append(item)
        if len(self.buffer) >= BATCH_SIZE:
            self.insert_batch_from_buffer(spider)
        return item

    def insert_batch_from_buffer(self, spider):
        """Insert buffer items into the database and clear the buffer."""
        try:
            if self.buffer:
                query = """
                    INSERT INTO y2020 (sbd, toan, van, ngoaiNgu, vatLy, hoaHoc, sinhHoc, diemTBTuNhien, lichSu, diaLy, gdcd, diemTBXaHoi)
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
                        for item in self.buffer]

                # Validate and clean the data before insertion
                cleaned_data = validate_and_clean_data(data)

                self.cursor.executemany(query, cleaned_data)
                self.db_connection.commit()
                print('Data pushed')
                self.buffer = []  # Clear the buffer
        except Exception as e:
            self.db_connection.rollback()
            spider.logger.error(f"Error inserting buffer batch: {e}")

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
