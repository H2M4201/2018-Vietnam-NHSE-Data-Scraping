import scrapy
import json
import pandas as pd


base_url = 'https://diemthi.vnanet.vn/Home/SearchBySobaodanh?code=*&nam=2023'
ID_BUFFER = 300

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

        # load province code, which is also being scrapped from another website
        with open('./THPT2023/spiders/province.json', 'r', encoding='utf-8') as json_file:
            province_code = list(json.load(json_file).keys())
        json_file.close()
        
        with open('max_id.txt', 'r') as file:
            max_id = [line.strip() for line in file if line.strip()]
        file.close()
        max_id = [int(id[2:]) + ID_BUFFER for id in max_id]
        max_id_with_province_code = dict(zip(province_code, max_id))

        # Loop through all provinces
        for p, mID in max_id_with_province_code.items():
            if int(p) != 4:
                continue
            for i in range(1, mID):
                url = base_url.replace('*', p + str(i).zfill(6))
                request = scrapy.Request(url=url, callback=self.parse)
                yield request


    def parse(self, response):
        data = json.loads(response.text)
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
                'diemTBXaHoi': result.get('KHXH')
            }

class AddMissingRecordSpider (scrapy.Spider):
    name = "AddMissing"
    allowed_domains = ["diemthi.vnanet.vn"]
    start_urls = []
    def start_requests(self):
        # load province code, which is also being scrapped from another website
        with open('possible_missing_id.txt', 'r') as file:
            possible_missing_id = [line.strip() for line in file if line.strip()]
        file.close()

        # Loop through all provinces
        for id in possible_missing_id:
            url = base_url.replace('*', id)
            request = scrapy.Request(url=url, callback=self.parse)
            yield request


    def parse(self, response):
        data = json.loads(response.text)
        for result in data.get('result', []):
            print(result)
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
                'diemTBXaHoi': result.get('KHXH')
            }