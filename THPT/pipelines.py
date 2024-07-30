# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from dotenv import load_dotenv
import os
import pymysql
from sqlalchemy import create_engine, Column, String, DECIMAL, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper
import ast

# READ FROM .env file
#---------------------------------
# Determine the root directory of your project
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Specify the path to the .env file
dotenv_path = os.path.join(project_root, '.env')

# Load the .env file
load_dotenv(dotenv_path)
#---------------------------------


# ASSIGN VALUES READ FROM .ENV FILE TO VARIABLES
#---------------------------------
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE"))

DB_USER =  os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
TARGET_YEARS = ast.literal_eval(os.getenv('TARGET_YEARS'))
#---------------------------------

Base = declarative_base()
metadata = MetaData()

def create_student_table(table_name):
    return Table(
        table_name,
        metadata,
        Column('sbd', String(10), primary_key=True),
        Column('toan', DECIMAL(10, 3)),
        Column('van', DECIMAL(10, 3)),
        Column('ngoaiNgu', DECIMAL(10, 3)),
        Column('vatLy', DECIMAL(10, 3)),
        Column('hoaHoc', DECIMAL(10, 3)),
        Column('sinhHoc', DECIMAL(10, 3)),
        Column('diemTBTuNhien', DECIMAL(10, 3)),
        Column('lichSu', DECIMAL(10, 3)),
        Column('diaLy', DECIMAL(10, 3)),
        Column('gdcd', DECIMAL(10, 3)),
        Column('diemTBXaHoi', DECIMAL(10, 3)),
        extend_existing=True
    )

DATABASE_URI = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DATABASE_URI)
metadata.bind = engine
Session = sessionmaker(bind=engine)


class ThptPipeline:
    def __init__(self, batch_size=BUFFER_SIZE):
        self.batch_size = batch_size
        self.buffer_by_year = {}
        self.db_connection = None
        self.cursor = None
        self.engine = engine
        
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
        if self.buffer_by_year:
            self.insert_batch_from_buffer(spider)
        self.db_connection.close()


    def process_item(self, item, spider):
        year = item['year']
        if year not in self.buffer_by_year:
            self.buffer_by_year[year] = []
        self.buffer_by_year[year].append(item)
        
        # Check if any of the year-based buffers exceed the batch size
        if any(len(items) >= self.batch_size for items in self.buffer_by_year.values()):
            self.insert_batch_from_buffer(spider)
        
        return item


    def insert_batch_from_buffer(self, spider):
        """Insert buffer items into the database and clear the buffer."""
        try:
            for year, items in list(self.buffer_by_year.items()):
                if items:
                    table_name = f'y{year}'
                    
                    student_table = create_student_table(table_name)
                    student_table.create(engine, checkfirst=True)
                    
                    # Clean and prepare data for insertion
                    cleaned_data = validate_and_clean_data(items)
                    query = f"""
                        INSERT INTO {table_name} (sbd, toan, van, ngoaiNgu, vatLy, hoaHoc, sinhHoc, diemTBTuNhien, lichSu, diaLy, gdcd, diemTBXaHoi)
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

                    self.cursor.executemany(query, [
                    (
                        item['sbd'],
                        item['toan'],
                        item['van'],
                        item['ngoaiNgu'],
                        item['vatLy'],
                        item['hoaHoc'],
                        item['sinhHoc'],
                        item['diemTBTuNhien'],
                        item['lichSu'],
                        item['diaLy'],
                        item['gdcd'],
                        item['diemTBXaHoi']
                    ) for item in cleaned_data
                ])
                    self.db_connection.commit()
                    print(f'Data for year {year} pushed')
                    
                    # Clear the buffer for this year
                    self.buffer_by_year[year] = []
                    
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
    for item in data:
        cleaned_record = {
            'sbd': item['sbd'],
            'toan': None if item['toan'] == '' else item['toan'],
            'van': None if item['van'] == '' else item['van'],
            'ngoaiNgu': None if item['ngoaiNgu'] == '' else item['ngoaiNgu'],
            'vatLy': None if item['vatLy'] == '' else item['vatLy'],
            'hoaHoc': None if item['hoaHoc'] == '' else item['hoaHoc'],
            'sinhHoc': None if item['sinhHoc'] == '' else item['sinhHoc'],
            'diemTBTuNhien': None if item['diemTBTuNhien'] == '' else item['diemTBTuNhien'],
            'lichSu': None if item['lichSu'] == '' else item['lichSu'],
            'diaLy': None if item['diaLy'] == '' else item['diaLy'],
            'gdcd': None if item['gdcd'] == '' else item['gdcd'],
            'diemTBXaHoi': None if item['diemTBXaHoi'] == '' else item['diemTBXaHoi']
        }
        cleaned_data.append(cleaned_record)

    return cleaned_data
