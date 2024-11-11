import json
import os
from datetime import datetime

import boto3
import pymysql
from dotenv import load_dotenv

load_dotenv()

# NCP Object Storage 설정
s3 = boto3.client('s3',
    endpoint_url='https://kr.object.ncloudstorage.com',
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_KEY')
)
bucket_name = os.getenv('BUCKET_NAME_2')

# NCP MySQL 연결 설정
ncp_db = pymysql.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('NCP_DB_NAME')
)

ncp_cursor = ncp_db.cursor()

# NCP MySQL의 모든 테이블 수집
ncp_cursor.execute("SHOW TABLES")
tables = [table[0] for table in ncp_cursor.fetchall()]

# 각 테이블의 데이터 수집
for table in tables:
    sql = f"SELECT * FROM {table}"
    ncp_cursor.execute(sql)
    data = ncp_cursor.fetchall()

    # JSON 형식으로 변환
    data_json = json.dumps([dict(zip([column[0] for column in ncp_cursor.description], row)) for row in data], default=str)

    # NCP Object Storage에 json 저장
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f'{table}_data_{timestamp}.json'  # Include table name in the file

    try:
        acl = 'private'
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=data_json, ACL=acl)
        print(f"Data from table '{table}' successfully stored in Object Storage with file name: {file_name} and ACL: {acl}")
        
        file_metadata = {
        "file_name": file_name,
        "table_name": table,
        "timestamp": timestamp,
        "acl": acl
    }
    except Exception as e:
        print(f"Error storing data from table '{table}' in Object Storage: {e}")

ncp_db.close()
