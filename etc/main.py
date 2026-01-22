import os
import pandas as pd
import pymysql
import urllib.parse
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. .env 파일 로드
load_dotenv()

# 2. 환경 변수에서 정보 가져오기
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", 3306))
FILE_PATH = "서울교통공사_지하철혼잡도정보_20251130.csv"

# 비밀번호 특수문자 인코딩 (SQLAlchemy용)
password_quoted = urllib.parse.quote_plus(DB_PASS)

# 3. DB 생성 (PyMySQL)
try:
    temp_conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        charset='utf8mb4'
    )
    with temp_conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4")
    temp_conn.close()
    print(f"'{DB_NAME}' 데이터베이스 준비 완료.")
except Exception as e:
    print(f"DB 생성 중 오류: {e}")

# 4. 데이터 적재 (Pandas & SQLAlchemy)
try:
    df = pd.read_csv(FILE_PATH, encoding='cp949')
    
    # 연결 문자열 구성
    engine_url = f"mysql+pymysql://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    engine = create_engine(engine_url)
    
    # 적재 실행
    df.to_sql('subway_data', con=engine, if_exists='replace', index=False)
    print("CSV 데이터 적재 성공!")

except Exception as e:
    print(f"적재 중 오류 발생: {e}")