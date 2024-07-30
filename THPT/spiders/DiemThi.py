import scrapy
import json
from dotenv import load_dotenv
import os
import ast

# READ FROM .env FILE
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
BASE_URL = os.getenv("BASE_URL")
ID_PADDING = int(os.getenv("ID_PADDING"))
PROVINCE_CODE_PATH = os.getenv("PROVINCE_CODE_PATH")
ESTIMATED_MAX_ID_PATH = os.getenv("ESTIMATED_MAX_ID_PATH")
POSSIBLE_MISSING_ID_PATH = os.path.normpath(os.path.join(project_root, os.getenv("POSSIBLE_MISSING_ID_PATH")))

TARGET_YEARS = ast.literal_eval(os.getenv('TARGET_YEARS'))

 # load province code, which is also being scrapped from another website
with open(PROVINCE_CODE_PATH, 'r', encoding='utf-8') as json_file:
    province_code = list(json.load(json_file).keys())
json_file.close()

class DiemthiSpider(scrapy.Spider):
    name = "DiemThi"
    allowed_domains = ["diemthi.vnanet.vn"]
    start_urls = []

    def start_requests(self):
        """
        - Because I cannot know exactly how many students each province has, so I checked which province
        has the most students, and how many. The number was about more than 111k
        ==> Therefore, I assume that every province would have arround 110k students at maximum.
        - Also, I notice that the first 2 digits of each student ID is the province code, and the rest
        is the real ID.

        Therefore, for each province, I will loop through the ids from 1 to MAX_STUDENT (which is
        around 110k). If one province really hits the last ID, then from that ID onwards, it will return
        NULL. So if I receive 10 continuous NULL, I will break and move on to the next province
        """
        # load estimated max ID of each province
        with open(ESTIMATED_MAX_ID_PATH, 'r') as file:
            estimated_max_id = [line.strip() for line in file if line.strip()]
        file.close()
        estimated_max_id = [int(id[2:]) + ID_PADDING for id in estimated_max_id]
        estimated_max_id_with_province_code = dict(zip(province_code, estimated_max_id))

        # Loop through all years. Then for each years, loop through all provinces
        for year in TARGET_YEARS:
            base_url_with_year = BASE_URL + str(year)
            for pCode, maxID in estimated_max_id_with_province_code.items():
                for id in range(1, maxID):
                    url = base_url_with_year.replace('*', pCode + str(id).zfill(6))
                    request = scrapy.Request(url=url, callback=self.parse)
                    request.meta['year'] = year
                    yield request


    def parse(self, response):
        data = json.loads(response.text)
        year = response.meta.get('year')
        for result in data.get('result', []):
            yield {
                'sbd': result.get('Code'),
                'toan': result.get('Toan'),
                'van': result.get('NguVan'),
                'ngoaiNgu': result.get('NgoaiNgu'),
                'vatLy': result.get('VatLi'),
                'hoaHoc': result.get('HoaHoc'),
                'sinhHoc': result.get('SinhHoc'),
                'diemTBTuNhien': result.get('KHTN'),
                'lichSu': result.get('LichSu'),
                'diaLy': result.get('DiaLi'),
                'gdcd': result.get('GDCD'),
                'diemTBXaHoi': result.get('KHXH'),
                'year': year
            }

class AddMissingRecordSpider (scrapy.Spider):
    name = "AddMissing"
    allowed_domains = ["diemthi.vnanet.vn"]
    start_urls = []

    def start_requests(self):
        # load province code, which is also being scrapped from another website
        with open(POSSIBLE_MISSING_ID_PATH, 'r', encoding='utf-8') as file:
            possible_missing_id = json.load(file)
        file.close()

        # Loop through all provinces
        for year in TARGET_YEARS:
            base_url_with_year = BASE_URL + str(year)
            for id in possible_missing_id[str(year)]:
                url = base_url_with_year.replace('*', id)
                request = scrapy.Request(url=url, callback=self.parse)
                request.meta['year'] = year
                yield request


    def parse(self, response):
        data = json.loads(response.text)
        year = response.meta.get('year')
        for result in data.get('result', []):
            yield {
                'sbd': result.get('Code'),
                'toan': result.get('Toan'),
                'van': result.get('NguVan'),
                'ngoaiNgu': result.get('NgoaiNgu'),
                'vatLy': result.get('VatLi'),
                'hoaHoc': result.get('HoaHoc'),
                'sinhHoc': result.get('SinhHoc'),
                'diemTBTuNhien': result.get('KHTN'),
                'lichSu': result.get('LichSu'),
                'diaLy': result.get('DiaLi'),
                'gdcd': result.get('GDCD'),
                'diemTBXaHoi': result.get('KHXH'),
                'year': year
            }