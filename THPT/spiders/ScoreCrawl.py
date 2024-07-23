import scrapy
import json

MAX_STUDENT = 110000
# in base url, the aterisk acts as a placeholder for the actual student ID
base_url = 'https://dantri.com.vn/thpt/1/0/99/*/2024/0.2/search-gradle.htm'
continuous_none_responses = 0

class DiemThiSpider(scrapy.Spider):
    name = 'diemthi'
    allowed_domains = ['dantri.com.vn']
    start_urls = []

    
    def start_requests(self):
        """
        - Because I cannot know exactly how many students each province has, so I checked which province
        has the most students, and how many. The number was about more than 109k
        ==> Therefore, I assume that every province would have arround 110k students at maximum.
        - Also, I notice that the first 2 digits of each student ID is the province code, and the rest
        is the real ID.

        Therefore, for each province, I will loop through the ids from 1 to MAX_STUDENT (which is
        around 110k). If one province really hits the last ID, then from that ID onwards, it will return
        NULL. So if I receive 10 continuous NULL, I will break and move on to the next province
        """

        # load province code, which is also being scrapped from another website
        with open('./THPT/spiders/province.json', 'r', encoding='utf-8') as json_file:
            province_code = json.load(json_file).keys()
        
        # Loop through all provinces
        for p in province_code:
            start = 1
            for i in range(start, MAX_STUDENT):
                if continuous_none_responses >= 10:
                    break
                url = base_url.replace('*', p + str(i).zfill(6))
                request = scrapy.Request(url=url, callback=self.parse)
                request.meta['continuous_none_responses'] = continuous_none_responses
                yield request

    def parse(self, response):
        #print(response)
        continuous_none_responses = response.meta['continuous_none_responses']

        try:
            data = json.loads(response.text)
            student = data.get('student', None)
        except json.JSONDecodeError:
            student = None

        if student is None:
            continuous_none_responses += 1
        else:
            continuous_none_responses = 0
            yield {
                'sbd': student.get('sbd'),
                'toan': student.get('toan'),
                'van': student.get('van'),
                'ngoaiNgu': student.get('ngoaiNgu'),
                'vatLy': student.get('vatLy'),
                'hoaHoc': student.get('hoaHoc'),
                'sinhHoc': student.get('sinhHoc'),
                'diemTBTuNhien': student.get('diemTBTuNhien'),
                'lichSu': student.get('lichSu'),
                'diaLy': student.get('diaLy'),
                'gdcd': student.get('gdcd'),
                'diemTBXaHoi': student.get('diemTBXaHoi')
            }

