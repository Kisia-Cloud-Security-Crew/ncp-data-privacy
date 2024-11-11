import csv
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime

import boto3
import pymysql
from dotenv import load_dotenv

load_dotenv()

# MySQL local DB 연결 설정
local_db = pymysql.connect(
    host=os.getenv('LOCAL_DB_HOST'),
    user=os.getenv('LOCAL_DB_USER'),
    password=os.getenv('LOCAL_DB_PASSWORD'),
    database=os.getenv('LOCAL_DB_NAME')
)

local_cursor = local_db.cursor()

# NCP Object Storage 설정
s3 = boto3.client('s3',
    endpoint_url='https://kr.object.ncloudstorage.com',
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_KEY')
)
bucket_name = os.getenv('BUCKET_NAME_2')

# Object Storage에서 데이터 수집 및 파일 형식에 따른 파싱
def download_data_from_object_storage(file_name):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response['Body'].read().decode('utf-8')
        
        if file_name.endswith('.json'):
            # JSON 배열 형식 파싱
            data = json.loads(file_content)
        elif file_name.endswith('.csv'):
            # CSV 파일 파싱
            data = []
            csv_reader = csv.DictReader(file_content.splitlines())
            for row in csv_reader:
                data.append(row)
        elif file_name.endswith('.xml'):
            # XML 파일 파싱
            root = ET.fromstring(file_content)
            data = [{child.tag: child.text for child in element} for element in root]
        else:
            print(f"Unsupported file format: {file_name}")
            return None
        return data
    except Exception as e:
        print(f"Error fetching or parsing data from Object Storage for {file_name}: {e}")
        return None


# Object Storage에서 파일의 ACL 권한 가져오기
def get_object_acl(file_name):
    try:
        acl_response = s3.get_object_acl(Bucket=bucket_name, Key=file_name)
        permissions = [grant['Permission'] for grant in acl_response['Grants']]
        acl = ', '.join(permissions)
        return acl
    except Exception as e:
        print(f"Error fetching ACL for {file_name}: {e}")
        return None

# 테이블 생성 후 데이터 저장
def insert_data_into_local_mysql(data, table_name, acl):
    if not data:
        print(f"No data to insert for table {table_name}.")
        return

    def determine_column_type(value):
        if isinstance(value, int):
            return "INT"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, str):
            return "LONGTEXT"
        else:
            return "LONGTEXT"

    # 테이블이 존재하면 삭제
    local_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # 테이블 생성
    first_row = data[0]
    columns = ", ".join([f"{col} {determine_column_type(first_row[col])}" for col in first_row.keys()])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}, acl LONGTEXT)"

    try:
        local_cursor.execute(create_table_sql)
        local_db.commit()
        print(f"Table '{table_name}' created.")

        # 테이블에 데이터 삽입
        insert_sql = f"INSERT INTO {table_name} ({', '.join(first_row.keys())}, acl) VALUES ({', '.join(['%s'] * len(first_row))}, %s)"
        
        for row in data:
            local_cursor.execute(insert_sql, list(row.values()) + [acl])
        
        local_db.commit()
        print(f"{len(data)} records inserted into local MySQL table '{table_name}'.")
    except Exception as e:
        print(f"Error inserting data into local MySQL: {e}")

def process_files_from_object_storage():
    # Object Storage Bucket 내의 파일 목록 불러오기
    response = s3.list_objects_v2(Bucket=bucket_name)
    files = [item['Key'] for item in response.get('Contents', [])]

    total_rows_inserted = 0
    for file_name in files:
        print(f"Processing file: {file_name}")
        table_name = file_name.split('_')[0]
        data = download_data_from_object_storage(file_name)

        if data:
            acl = get_object_acl(file_name)
            insert_data_into_local_mysql(data, table_name, acl)
            total_rows_inserted += len(data)

    print(f"Total rows inserted: {total_rows_inserted}")
    local_db.close()

process_files_from_object_storage()
