import pandas as pd
import re
from sqlalchemy import text

class SubwayManager:
    def __init__(self, engine):
        self.engine = engine

    def drop_table(self, table_name):
        """기존 테이블 삭제"""
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
        print(f"🗑️ 기존 테이블 [{table_name}] 삭제 완료.")

    def load_csv_bulk(self, file_list, target_table, encoding='cp949'):
        """파일 리스트를 순회하며 날짜 컬럼을 추가해 통합 적재"""
        if not file_list:
            return

        # 1. 시작 전 테이블 삭제
        self.drop_table(target_table)

        for i, file_path in enumerate(file_list):
            try:
                # 2. 파일명에서 날짜(8자리 숫자) 추출
                # 예: "서울교통공사_지하철혼잡도정보_20251130.csv" -> "20251130"
                date_match = re.search(r'\d{8}', file_path)
                file_date = date_match.group() if date_match else "Unknown"

                # 3. 데이터 읽기
                df = pd.read_csv(file_path, encoding=encoding)
                
                # 4. 전처리: 날짜 컬럼 추가 및 컬럼명 정제
                df['base_date'] = file_date  # 날짜 컬럼 추가
                df.columns = [col.strip().replace(" ", "_") for col in df.columns]

                # 5. 적재 (첫 파일은 replace, 이후 append)
                mode = 'replace' if i == 0 else 'append'
                df.to_sql(name=target_table, con=self.engine, if_exists=mode, index=False)
                
                print(f"✅ {file_path} ([{file_date}]) -> 적재 완료")
                
            except Exception as e:
                print(f"❌ {file_path} 처리 중 오류: {e}")