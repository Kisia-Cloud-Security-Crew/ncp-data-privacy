import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime

import boto3
import pymysql
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('API_KEY')
access_key = os.getenv('ACCESS_KEY')
secret_key = os.getenv('SECRET_KEY')
bucket = os.getenv('BUCKET_NAME')
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# NCP Object Storage 설정
s3 = boto3.client('s3', endpoint_url='https://kr.object.ncloudstorage.com',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)
bucket_name = bucket

# NCP Cloud Log Analytics 설정
uri = f'/api/kr-v1/logs/search'
api_url = f'https://cloudloganalytics.apigw.ntruss.com{uri}'

# MySQL 연결 설정
db = pymysql.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

cursor = db.cursor()

# 시그니처 생성
def make_signature(method, uri, access_key, secret_key):
    timestamp = str(int(time.time() * 1000))
    message = f"{method} {uri}\n{timestamp}\n{access_key}"
    signing_key = base64.b64encode(hmac.new(secret_key.encode(), message.encode(), hashlib.sha256).digest())
    return signing_key, timestamp

# 수집된 로그를 Object Storage에 저장
def upload_log_to_object_storage(log_data, log_type):
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        date_folder = datetime.now().strftime('%Y-%m-%d')
        file_name = f'{log_type}_logs_{timestamp}.json'
        s3_key = f'{log_type}/{date_folder}/{file_name}'

        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(log_data))
        print(f'로그가 Object Storage에 저장되었습니다: {s3_key}')
        return s3_key
    except Exception as e:
        print(f'오류 발생: {e}')
        return None

# Object Storage에서 로그 파일 가져오기
def download_log_from_object_storage(file_name):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        log_data = json.loads(response['Body'].read().decode('utf-8'))
        return log_data
    except Exception as e:
        print(f'오류 발생: {e}')
        return None

# MySQL에 로그 저장
def store_log_in_mysql(log_data, log_type_table):
    try:
        for log in log_data:
            log_detail = log.get('logDetail', 'No detail')
            log_time = log.get('logTime')
            log_type = log.get('logType', 'Unknown')
            server_name = log.get('servername', 'Unknown')

            if "{name=" in server_name:
                server_name = server_name.split("{name=")[-1].split("}")[0]

            if log_time:
                log_time = datetime.fromtimestamp(int(log_time) / 1000)

            sql = f"INSERT INTO {log_type_table} (log_time, log_type, servername, log_detail) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (log_time, log_type, server_name, log_detail))
        db.commit()
        print(f'로그가 {log_type_table} 테이블에 저장되었습니다.')
    except Exception as e:
        print(f'Error inserting log: {e}')

# 로그 수집 후 Object Storage과 MySQL에 저장
def collect_logs_and_store(log_type, log_type_table):
    signature, timestamp = make_signature("POST", uri, access_key, secret_key)
    headers = {
        'x-ncp-apigw-timestamp': timestamp,
        'x-ncp-apigw-api-key': api_key,
        'x-ncp-iam-access-key': access_key,
        'x-ncp-apigw-signature-v2': signature.decode(),
        'Content-Type': 'application/json'
    }

    body = {
        "logTypes": log_type,
        "pageSize": 100,  # 한 번에 가져올 로그 개수
        "pageNo": 1  # 페이지 번호
    }

    response = requests.post(api_url, headers=headers, data=json.dumps(body))

    if response.status_code == 200:
        logs = response.json().get('result', {}).get('searchResult', [])
        if logs:
            file_name = upload_log_to_object_storage(logs, log_type)
            if file_name:
                log_data = download_log_from_object_storage(file_name)
                if log_data:
                    store_log_in_mysql(log_data, log_type_table)
        else:
            print("로그가 없습니다.")
    else:
        print(f"오류 발생: {response.status_code}, {response.text}")

collect_logs_and_store("cdb_mysql_error", "ncp_log_table")
collect_logs_and_store("cdb_mysql_audit", "ncp_log_table")
collect_logs_and_store("cdb_mysql_slow", "ncp_log_table")
collect_logs_and_store("audit_log", "ncp_log_table")

db.close()
