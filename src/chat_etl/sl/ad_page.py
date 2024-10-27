import pandas as pd
import json
import streamlit as st
import time
import datetime

# 전역 변수 초기화
date = None

@st.cache_data(ttl=120)
def load_data():
    global date
    # file 읽어오기
    
    # date를 문자열로 변환하고 "-"를 제거
    date = date.isoformat().replace("-", "")
    d = pd.read_parquet(f"/home/ubuntu/data/tmp/{date}")
    d_json = d.to_json(orient='records')  # JSON 변환
    return json.loads(d_json)  # JSON 포맷으로 변환하여 반
    
# request_user 선택
def select_request_user(unique_id):
    data = load_data()
    df = pd.DataFrame(data)
    print("===="*30)
    print(df) 
    options = ["모든 사용자"] + df['request_senderId'].unique().tolist()

    # 사용자 선택
    user = st.selectbox(
        "request_senderId 선택",
        options,
        index=0,  # 기본값은 "모든 사용자"
        placeholder="request_senderId를 선택해주세요.",
        key=f"select_request_user_{unique_id}"  # 고유한 key 값 설정
    )

    return user

def select_num(user, unique_id):
    data = load_data()
    df = pd.DataFrame(data)

    # 선택된 user에 따라 num 값을 필터링
    if user != "모든 사용자":
        filtered_df = df[df['request_senderId'] == user]
        options = ["모든 번호"] + filtered_df['request_senderId'].unique().tolist()
    else:
        options = ["모든 번호"] + df['request_senderId'].unique().tolist()  # 전체 번호 표시

    return options

def show_table(user, unique_id):
    data = load_data()
    df = pd.DataFrame(data)

    # 사용자가 선택한 user와 num에 따라 데이터 필터링
    if user != "모든 사용자":
        df = df[df['request_senderId'] == user]

    # 필터링된 테이블 출력
    if not df.empty:
        st.write(df)
    else:
        st.write("선택한 조건에 맞는 데이터가 없습니다.")

def show_bar(x='request_senderId'):
    data = load_data()
    df = pd.DataFrame(data)

    import matplotlib.pyplot as plt
    import seaborn as sns
    grouped_df = df.groupby(x).size().reset_index(name='count')
    grouped_df = grouped_df[[x, 'count']]
    # Streamlit에서 그룹화된 데이터프레임을 시각화
    st.write("Grouped Data by 'id':")
    st.dataframe(grouped_df)

    # 막대그래프 그리기
    fig, ax = plt.subplots()
    sns.barplot(x=grouped_df['request_senderId'], y=grouped_df['count'])
    ax.set_xlabel('id')
    ax.set_ylabel('request_senderId')
    ax.set_title('ID BY REQUEST COUNT')

    # Streamlit에 그래프 표시
    st.pyplot(fig)

def main():
    global date  # 전역 변수를 사용하겠다고 선언

    date = st.date_input("Date", value=datetime.date(2024, 1, 1))  # 전역 변수에 값 할당
    
    # 고유한 키 식별자를 사용하여 사용자와 num을 선택
    selected_user = select_request_user("user_table_1")
    selected_num = select_num(selected_user, "num_table_1")

    # 선택한 값에 맞는 테이블을 보여줌
    show_table(selected_user, "table_1")
    show_bar()

# Streamlit 애플리케이션 실행
if __name__ == "__main__":
    main()
