import streamlit as st
import pandas as pd
from db_connector import DBConnector

def get_data(query):
    connector = DBConnector()
    engine = connector.get_engine()
    return pd.read_sql(query, engine)

def format_db_column(time_str):
    """'05시30분' -> '5시30분' 변환"""
    return time_str[1:] if time_str.startswith("0") else time_str

def get_route_info(line, start_st, end_st):
    query = f"SELECT * FROM subway_distance WHERE 호선 = '{line}'"
    df_all = get_data(query)
    if df_all.empty: return None, None

    try:
        df_all['역명'] = df_all['역명'].str.strip()
        # 노선도에서 동일 역명 중복 제거
        df_all = df_all.drop_duplicates(subset=['역명'])
        
        idx_start = df_all[df_all['역명'] == start_st.strip()].index[0]
        idx_end = df_all[df_all['역명'] == end_st.strip()].index[0]
    except: return None, None

    if idx_start <= idx_end:
        route_df = df_all.iloc[idx_start:idx_end+1].copy()
        direction = "내선" if line == "2" else "하선"
    else:
        route_df = df_all.iloc[idx_end:idx_start+1].iloc[::-1].copy()
        direction = "외선" if line == "2" else "상선"

    route_df['역간거리(km)'] = pd.to_numeric(route_df['역간거리(km)'], errors='coerce').fillna(0)
    route_df['누적거리'] = route_df['역간거리(km)'].cumsum()
    
    def to_min(t_str):
        if not t_str or ':' not in str(t_str): return 0
        p = t_str.split(':')
        return int(p[0]) + int(p[1])/60
    
    route_df['누적시간'] = route_df['소요시간'].apply(to_min).cumsum().round(0).astype(int)
    return route_df, direction

def main():
    st.set_page_config(page_title="지하철 혼잡도 가이드", layout="wide")
    st.title("🚇 지하철 실시간 노선도 & 혼잡도")

    # 사이드바 설정
    st.sidebar.header("🔍 설정")
    day_type = st.sidebar.selectbox("요일", ["평일", "토요일", "일요일"])
    line_input = st.sidebar.selectbox("호선", [str(i) for i in range(1, 10)])
    start_st = st.sidebar.text_input("출발역", "서울역")
    end_st = st.sidebar.text_input("도착역", "청량리")
    
    time_options = [f"{h:02d}시{m}분" for h in range(5, 24) for m in ["00", "30"] if not (h==5 and m=="00")]
    selected_time = st.sidebar.selectbox("시간", time_options)
    
    db_col = format_db_column(selected_time)
    route_df, auto_dir = get_route_info(line_input, start_st, end_st)

    if route_df is not None:
        st.subheader(f"📍 {line_input}호선 [{auto_dir}] 노선도")
        station_names = "('" + "','".join(route_df['역명'].tolist()) + "')"
        
        # SQL 쿼리 (GROUP BY로 중복 제거 및 백틱 처리)
        query_con = f"""
            SELECT 출발역, ROUND(AVG(`{db_col}`), 0) as congestion 
            FROM subway_congestion 
            WHERE 출발역 IN {station_names} 
              AND 호선 = '{line_input}호선' 
              AND 요일구분 = '{day_type}' 
              AND 상하구분 = '{auto_dir}'
            GROUP BY 출발역
        """
        
        try:
            df_con = get_data(query_con)
            final_df = pd.merge(route_df, df_con, left_on='역명', right_on='출발역', how='left').fillna(0)

            st.write("---")
            
            # --- HTML 렌더링 시작 ---
            # 1. 스크롤 가능한 컨테이너 태그
            html_code = '<div style="display: flex; overflow-x: auto; white-space: nowrap; padding: 20px; background: #f9f9f9; border-radius: 10px; border: 1px solid #ddd;">'
            
            for i, row in final_df.iterrows():
                val = int(row['congestion'])
                # 혼잡도에 따른 색상
                color = "#00CC96" if val < 35 else "#FECB52" if val < 70 else "#EF553B"
                if val == 0: color = "#D3D3D3"
                
                # 역 노드 추가
                html_code += f'''
                <div style="display: inline-block; min-width: 90px; text-align: center; vertical-align: top;">
                    <div style="font-size: 13px; font-weight: bold; color: #333; margin-bottom: 8px;">{row['역명']}</div>
                    <div style="width: 20px; height: 20px; background: {color}; border-radius: 50%; margin: 0 auto; border: 3px solid #fff; box-shadow: 0 2px 5px rgba(0,0,0,0.2);"></div>
                    <div style="font-size: 12px; margin-top: 8px; font-weight: bold;">{val}%</div>
                    <div style="font-size: 10px; color: #888;">{row['누적시간']}분</div>
                </div>
                '''
                # 연결선 추가
                if i < len(final_df) - 1:
                    html_code += '<div style="display: inline-block; width: 40px; border-top: 2px solid #ccc; margin-top: 40px;"></div>'
            
            html_code += '</div>'
            
            # [중요] 옵션을 True로 주어야 코드가 아닌 이미지로 나옵니다.
            st.markdown(html_code, unsafe_allow_html=True)
            st.write("---")
            # --- HTML 렌더링 끝 ---

            # 하단 지표
            c1, c2, c3 = st.columns(3)
            c1.metric("총 거리", f"{final_df['누적거리'].iloc[-1]:.2f}km")
            c2.metric("총 소요시간", f"{final_df['누적시간'].iloc[-1]}분")
            c3.metric("평균 혼잡도", f"{int(final_df['congestion'].mean())}%")

        except Exception as e:
            st.error(f"SQL 오류: {e}")
    else:
        st.warning("경로 정보를 불러올 수 없습니다.")

if __name__ == "__main__":
    main()