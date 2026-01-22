import glob
import os
from db_connector import DBConnector
from subway_manager import SubwayManager

def main():
    connector = DBConnector()
    engine = connector.get_engine()
    manager = SubwayManager(engine)

    # 1. 혼잡도 관련 모든 파일 찾기
    congestion_files = glob.glob("서울교통공사_지하철혼잡도정보*.csv")
    
    # 2. 하나의 'subway_congestion' 테이블에 모두 저장
    if congestion_files:
        print(f"📂 총 {len(congestion_files)}개의 혼잡도 파일을 통합 적재합니다.")
        manager.load_csv_bulk(congestion_files, "subway_congestion")

    # 3. 역간거리 및 소요시간 정보 (단일 테이블)
    dist_file = "서울교통공사_역간거리_및_소요시간_정보.csv"
    if os.path.exists(dist_file):
        manager.load_csv_bulk([dist_file], "subway_distance")

if __name__ == "__main__":
    main()