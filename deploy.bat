@echo on
pip install -r requirements.txt
scrapy crawl DiemThi
python ./THPT/FindMissingUtil.py
scrapy crawl AddMissing