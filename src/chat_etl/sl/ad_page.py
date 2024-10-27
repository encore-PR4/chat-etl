import pandas as pd
import json
import streamlit as st

def load_data():
    # file 읽어오기
    # parquet 파일을 json으로 변경
    d = pd.read_parquet("/home/kim1/data/tmp")
    d_json = d.to_json(orient='records')  # JSON 변환
    return json.loads(d_json)  # JSON 포맷으로 변환하여 반
    
# request_user 선택
def select_request_user(unique_id):
    data = load_data()
    df = pd.DataFrame(data)
    options = ["모든 사용자"] + df['specific_path'].unique().tolist()

    # 사용자 선택
    user = st.selectbox(
        "request user 선택",
        options,
        index=0,  # 기본값은 "모든 사용자"
        placeholder="request_user를 선택해주세요.",
        key=f"select_request_user_{unique_id}"  # 고유한 key 값 설정
    )

    return user

def select_num(user, unique_id):
    data = load_data()
    df = pd.DataFrame(data)

    # 선택된 user에 따라 num 값을 필터링
    if user != "모든 사용자":
        filtered_df = df[df['specific_path'] == user]
        options = ["모든 번호"] + filtered_df['specific_path'].unique().tolist()
    else:
        options = ["모든 번호"] + df['specific_path'].unique().tolist()  # 전체 번호 표시

    return options

def show_table(user, unique_id):
    data = load_data()
    df = pd.DataFrame(data)

    # 사용자가 선택한 user와 num에 따라 데이터 필터링
    if user != "모든 사용자":
        df = df[df['specific_path'] == user]

    # 필터링된 테이블 출력
    if not df.empty:
        st.write(df)
    else:
        st.write("선택한 조건에 맞는 데이터가 없습니다.")

# Streamlit 애플리케이션 실행
try:
    if __name__ == "__main__":
        # 고유한 키 식별자를 사용하여 사용자와 num을 선택
        selected_user = select_request_user("user_table_1")
        selected_num = select_num(selected_user, "num_table_1")

        # 선택한 값에 맞는 테이블을 보여줌
        show_table(selected_user, "table_1")

except ConnectionError as e:
    st.error("서버가 불안정하여 DB에 연결할 수 없습니다. 나중에 다시 시도해주세요.")