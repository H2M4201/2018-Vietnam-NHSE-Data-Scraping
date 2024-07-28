import pandas as pd
import pymysql
from sqlalchemy import create_engine
import json

host='localhost'
user='root'
pwd='123456'
db='THPT'

province_code_filepath = './THPT2023/spiders/province.json'
score_filepath = 'score_2020.json'
missing_score_path = 'missing.json'

def insert_raw_data_into_db():
    conn = pymysql.connect(host=host, user=user, password=pwd, db=db)

    df = pd.read_json(score_filepath)
    df['sbd'] = [str(i) if len(str(i)) == 8 else '0' + str(i) for i in list(df['sbd'])]

    # Create a connection engine
    engine = create_engine(f'mysql+pymysql://{user}:{pwd}@{host}/{db}')

    # Transfer data to MySQL
    df.to_sql(name='Y2019', con=engine, if_exists='replace', index=False)

    print(f'Data has been transferred to the Y2019 table in the {db} database.')


def get_province_code_from_file():
    with open(province_code_filepath, 'r', encoding='utf-8') as json_file:
        province_code = list(json.load(json_file).keys())
    json_file.close()

    return province_code


def query_max_id_from_database():
    conn = pymysql.connect(host=host, user=user, password=pwd, db=db)
    cursor = conn.cursor()

    query = """
        select max(sbd) from y2020 where sbd like '*%';
    """

    max_ids = []
    province_code = get_province_code_from_file()
    for p in province_code:
        cursor.execute(query.replace('*', p))
        id = cursor.fetchone()
        max_ids.append(id[0])

    with open('max_ID_by_province_2020.txt', 'w') as f:
        for m in max_ids:
            f.write(f"{m}\n")
    f.close()


def get_max_id_from_file():
    with open('max_ID_by_province_2020.txt', 'r') as file:
        max_id = [line.strip() for line in file if line.strip()]

    return max_id

def find_possible_missing_id():
    conn = pymysql.connect(host=host, user=user, password=pwd, db=db)
    cursor = conn.cursor()

    cursor.execute('SET @@cte_max_recursion_depth = 100000000;')
    query = """
    WITH RECURSIVE sequence AS (
    SELECT '$000001' AS id
    UNION ALL
    SELECT LPAD(CAST(CAST(id AS UNSIGNED) + 1 AS CHAR), 8, '0')
    FROM sequence
    WHERE id < '*'
    )
    SELECT sequence.id
    FROM sequence
    LEFT JOIN y2020 ON sequence.id = y2020.sbd
    WHERE y2020.sbd IS NULL;
    """

    max_ids = get_max_id_from_file()
    province_code = get_province_code_from_file()
    max_id_by_province = dict(zip(province_code, max_ids))


    all_missing_id = []
    for pCode, mID in max_id_by_province.items():
        cursor.execute(query.replace('*', mID).replace('$', pCode))
        missingID = list(cursor.fetchall())
        print(pCode, ': ', len(missingID))
        all_missing_id += missingID

    print(len(all_missing_id))

    with open('./THPT2023/spiders/possible_missing_id.txt', 'w') as f:
        for m in all_missing_id:
            f.write(f"{m[0]}\n")
    f.close()

def insert_missing_data_into_db():
    conn = pymysql.connect(host=host, user=user, password=pwd, db=db)

    df = pd.read_json(missing_score_path)
    df['sbd'] = [str(i) if len(str(i)) == 8 else '0' + str(i) for i in list(df['sbd'])]

    # Create a connection engine
    engine = create_engine(f'mysql+pymysql://{user}:{pwd}@{host}/{db}')

    # Transfer data to MySQL
    df.to_sql(name='Y2019', con=engine, if_exists='append', index=False)

    print(f'Data has been added to the Y2019 table in the {db} database.')

if __name__ == '__main__':
    # insert_raw_data_into_db()
    query_max_id_from_database()
    get_province_code_from_file()
    find_possible_missing_id()
    # insert_missing_data_into_db()