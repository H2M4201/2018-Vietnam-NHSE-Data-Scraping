import pandas as pd
import pymysql
from sqlalchemy import create_engine
import json
import os
from dotenv import load_dotenv
import ast

# READ FROM .env file
#---------------------------------
# Determine the root directory of your project
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Specify the path to the .env file
dotenv_path = os.path.join(project_root, '.env')

# Load the .env file
load_dotenv(dotenv_path)
#---------------------------------


# ASSIGN VALUES READ FROM .ENV FILE TO VARIABLES
#---------------------------------
PROVINCE_CODE_PATH = os.getenv("PROVINCE_CODE_PATH")
TARGET_YEARS = ast.literal_eval(os.getenv('TARGET_YEARS'))

DB_USER =  os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

POSSIBLE_MISSING_ID_PATH = os.path.normpath(os.path.join(project_root, os.getenv("POSSIBLE_MISSING_ID_PATH")))
print(POSSIBLE_MISSING_ID_PATH)
#---------------------------------

conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)

with open(PROVINCE_CODE_PATH, 'r', encoding='utf-8') as json_file:
    province_code = list(json.load(json_file).keys())
json_file.close()


def query_all_max_id_from_database():
    cursor = conn.cursor()
    alL_max_id = {}

    for year in TARGET_YEARS:
        table_name = f'y{year}'
        max_id_by_year = []
        for pCode in province_code:
            query = f"""
                select max(sbd) from {table_name} where sbd like '{pCode}%';
            """
            cursor.execute(query)
            id = cursor.fetchone()
            max_id_by_year.append(id[0])

        alL_max_id[year] = max_id_by_year

    cursor.close()

    return alL_max_id

def find_possible_missing_id_by_province(cursor, table_name, pCode, maxID):
    cursor.execute('SET @@cte_max_recursion_depth = 65000000;')
    query = f"""
        WITH RECURSIVE sequence AS (
        SELECT '{pCode}000001' AS id
        UNION ALL
        SELECT LPAD(CAST(CAST(id AS UNSIGNED) + 1 AS CHAR), 8, '0')
        FROM sequence
        WHERE id <= '{maxID}'
        )
        SELECT sequence.id
        FROM sequence
        LEFT JOIN {table_name} ON sequence.id = {table_name}.sbd
        WHERE {table_name}.sbd IS NULL;
    """
    cursor.execute(query)
    missing_id_by_province = list(cursor.fetchall())

    return missing_id_by_province



def find_possible_missing_id():
    cursor = conn.cursor()
    all_possible_missing_id = {}
    all_max_id = query_all_max_id_from_database()

    for year in TARGET_YEARS:
        table_name = f'y{year}'
        max_id_by_year = all_max_id[year]
        possible_missing_id_by_year = []
    
        for pCode, maxID in dict(zip(province_code, max_id_by_year)).items():
            missing_id_by_province = find_possible_missing_id_by_province(cursor, table_name, pCode, maxID)
            print(year, ':', pCode, ': ', len(missing_id_by_province))
            possible_missing_id_by_year += missing_id_by_province

        print(year, len(possible_missing_id_by_year))
        all_possible_missing_id[year] = possible_missing_id_by_year

    conn.close()

    with open(POSSIBLE_MISSING_ID_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_possible_missing_id, f)
    f.close()

if __name__ == '__main__':
    with open(POSSIBLE_MISSING_ID_PATH, 'r', encoding='utf-8') as f:
        x = json.load(f)
    print(type(x['2018'][0]))