import csv
import datetime
import os
import re

import mysql.connector
from dotenv import load_dotenv

# Load environment variables for database configuration
load_dotenv()

db_config = {
    'host': os.getenv('LOCAL_DB_HOST'),
    'user': os.getenv('LOCAL_DB_USER'),
    'password': os.getenv('LOCAL_DB_PASSWORD'),
    'database': os.getenv('LOCAL_DB_NAME')
}

# Define patterns for sensitive data
patterns = {
    "주민등록번호": r"(\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])[-\s]*[1-4][\d*]{6})",
    "휴대전화 및 집 전화": r"((?:\(?0(?:[1-9]{2})\)?[-\s]?)?(?:\d{3,4})[-\s]?\d{4})",
    "계좌/카드번호": r"((\d{4}[-\s]?\d{6}[-\s]?\d{4,5})|(\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}))",
    "이메일": r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4})",
    "사업자등록번호": r"((1[1-6]|[2-9][0-9])-?(\d{2})-?\d{5})",
    "외국인등록번호": r"(\d{2}[01]\d[0123]\d[-\s]?[5678]\d{6})",
    "여권번호": r"([DMORS]\d{8}|(AY|BS|CB|CS|DG|EP|GB|GD|GG|GJ|GK|GN|GP|GS|GW|GY|HD|IC|JB|JG|JJ|JN|JR|JU|KJ|KN|KR|KY|MP|NW|SC|SJ|SM|SQ|SR|TJ|TM|UL|YP|YS)\d{7})",
    "운전면허": r"(서울|대전|대구|부산|광주|울산|인천|제주|강원|경기|충북|충남|전남|전북|경북|경남)[\s]{0,2}\d{2}[-\s]?\d{6}[-\s]?\d{2}",
    "건강보험번호": r"([1-9]-[0-6]\d{9})"
}

# Function to detect sensitive data in text
def detect_sensitive_data(text):
    detected_data = {}
    for data_type, pattern in patterns.items():
        matches = re.findall(pattern, text)
        if matches:
            detected_data[data_type] = matches
    return detected_data

# Function to scan the database for sensitive data
def scan_database():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]

    results = []

    for table in tables:
        cursor.execute(f"DESCRIBE {table}")
        columns = [column[0] for column in cursor.fetchall()]

        for column in columns:
            cursor.execute(f"SELECT `{column}` FROM `{table}`")
            rows = cursor.fetchall()

            for row in rows:
                if row[0] is not None:
                    text = str(row[0])
                    detected = detect_sensitive_data(text)
                    if detected:
                        for data_type, matches in detected.items():
                            for match in matches:
                                # Ensure each item added to results is a dictionary
                                results.append({
                                    "Table.Column": f"{table}.{column}",
                                    "Data_Type": data_type,
                                    "Match": match,
                                    "Sensitive": "민감"
                                })

    cursor.close()
    conn.close()

    return results

# Execute the scan and display results
scan_results = scan_database()

now = datetime.datetime.now()
dt = now.strftime("%Y-%m-%d_%H-%M-%S")
csv_filename = f"{dt}_sensitive_data.csv"

with open(csv_filename, "w", encoding="utf-8-sig", newline="") as csvfile:
    fieldnames = ["Table.Column", "Data_Type", "Match", "Sensitive"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for result in scan_results:
        writer.writerow(result)

print(f"Results have been saved to {csv_filename}.")