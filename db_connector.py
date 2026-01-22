import os
import urllib.parse
import pymysql
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

class DBConnector:
    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASS")
        self.db_name = os.getenv("DB_NAME")
        self.port = int(os.getenv("DB_PORT", 3306))
        
        # 비밀번호 인코딩 (SQLAlchemy용)
        self.encoded_password = urllib.parse.quote_plus(self.password)

    def _create_db_if_not_exists(self):
        """데이터베이스(스키마)가 없으면 생성"""
        conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port
        )
        try:
            with conn.cursor() as cursor:
                # DB_NAME이 없을 경우 생성
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name} CHARACTER SET utf8mb4")
            conn.commit()
            print(f"📡 데이터베이스 '{self.db_name}' 확인/생성 완료.")
        finally:
            conn.close()

    def get_engine(self):
        # 1. 먼저 DB 존재 여부 체크 및 생성
        self._create_db_if_not_exists()
        
        # 2. SQLAlchemy 엔진 생성
        url = f"mysql+pymysql://{self.user}:{self.encoded_password}@{self.host}:{self.port}/{self.db_name}"
        return create_engine(url)