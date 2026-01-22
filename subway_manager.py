import pandas as pd
from sqlalchemy import text

class SubwayManager:
    def __init__(self, engine):
        self.engine = engine

    # 1. 적재 (Load)
    def load_csv(self, file_path, table_name):
        try:
            df = pd.read_csv(file_path, encoding='cp949')
            df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
            print(f"✅ '{table_name}' 적재 성공")
        except Exception as e:
            print(f"❌ 적재 에러: {e}")

    # 2. 조회 (Read)
    def fetch_all(self, table_name):
        return pd.read_sql(f"SELECT * FROM {table_name}", self.engine)

    # 3. 수정/실행 (Update/Execute)
    def execute(self, sql_query):
        with self.engine.begin() as conn:
            conn.execute(text(sql_query))
            print("✅ 쿼리 실행 성공")

    # 4. 삭제 (Drop Table)
    def drop_table(self, table_name):
        self.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"🔥 '{table_name}' 테이블 삭제 완료")