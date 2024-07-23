# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import os


class ThptPipeline:
    def open_spider(self, spider):
        self.filepath = 'filtered_student_scores.json'
        # Create the file if it doesn't exist
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'a', encoding='utf-8') as f:
                json.dump([], f)
    
    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        # Load existing data
        with open(self.filepath, 'a', encoding='utf-8') as f:
            data = json.load(f)
            data.append(dict(item))
            f.seek(0)
            json.dump(data, f, ensure_ascii=False, indent=4)
        return item
