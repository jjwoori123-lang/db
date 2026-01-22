from db_connector import DBConnector
from subway_manager import SubwayManager

def main():
    # 1. DB 연결 엔진 가져오기
    connector = DBConnector()
    engine = connector.get_engine()

    # 2. 매니저 객체 생성
    manager = SubwayManager(engine)

    # 3. 작업 수행 (원하는 기능만 주석 해제하여 사용)
    FILE_PATH = "서울교통공사_지하철혼잡도정보_20251130.csv"
    TABLE_NAME = "subway_data"

    # [A] 데이터 적재
    manager.load_csv(FILE_PATH, TABLE_NAME)

    # [B] 데이터 조회
    # df = manager.fetch_all(TABLE_NAME)
    # print(df.head())

    # [C] 테이블 삭제 (Drop)
    # manager.drop_table(TABLE_NAME)

if __name__ == "__main__":
    main()