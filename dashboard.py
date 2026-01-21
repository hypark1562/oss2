import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import time
from datetime import datetime

# -----------------------------------------------------------------------------
# 1. 페이지 설정 (브라우저 탭 설정)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="LoL Challenger Data Pipeline",
    page_icon="⚙️",
    layout="wide"
)

# -----------------------------------------------------------------------------
# 2. 유틸리티 함수: 데이터 로드 & 파이프라인 상태 체크
# -----------------------------------------------------------------------------
DATA_PATH = "data/processed/cleaned_data.csv"

@st.cache_data
def load_data():
    if not os.path.exists(DATA_PATH):
        return None, None
   
    mod_time = os.path.getmtime(DATA_PATH)
    last_updated = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
    
    df = pd.read_csv(DATA_PATH)

    df['win_rate'] = (df['wins'] / (df['wins'] + df['losses']) * 100).round(1)
    df['total_games'] = df['wins'] + df['losses']
    df = df[df['win_rate'] <= 100] 
    
    return df, last_updated

df, last_updated = load_data()

# -----------------------------------------------------------------------------
# 3. 사이드바: 컨트롤 패널
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Dashboard Control")
    st.markdown("데이터 엔지니어링 포트폴리오용 대시보드입니다.")

    if df is not None:
        min_lp = int(df['leaguePoints'].min())
        max_lp = int(df['leaguePoints'].max())
        target_lp = st.slider("최소 점수 필터 (LP)", min_lp, max_lp, min_lp)
        
        filtered_df = df[df['leaguePoints'] >= target_lp]
    else:
        filtered_df = None

    st.markdown("---")
    st.markdown("### 👨‍💻 Developer Info")
    st.info("Developed by **HeeYeon**\n\nStack: Python, Riot API, AWS(Pre), Streamlit")

# -----------------------------------------------------------------------------
# 4. 메인 화면: 현업 스타일 레이아웃
# -----------------------------------------------------------------------------

st.title("📊 LoL Challenger Analytics Dashboard")
st.markdown("챌린저 티어 유저들의 게임 데이터를 분석하여 **승률 패턴**과 **상위 랭커**를 식별합니다.")

if df is not None:
    st.markdown(f"""
    <div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px; margin-bottom: 20px;">
        <span style="color: green; font-weight: bold;">● System Online</span> 
        | 🔄 Data Last Updated: <b>{last_updated}</b>
        | 📂 Total Records: <b>{len(df):,} Rows</b>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Target Users", f"{len(filtered_df)}명", help="필터링된 분석 대상 유저 수")
    with col2:
        avg_lp = filtered_df['leaguePoints'].mean()
        st.metric("Avg League Points", f"{int(avg_lp):,} LP")
    with col3:
        avg_wr = filtered_df['win_rate'].mean()
        st.metric("Avg Win Rate", f"{avg_wr:.1f}%", delta_color="normal")
    with col4:
        heavy_user = filtered_df.loc[filtered_df['total_games'].idxmax()]
        st.metric("Max Games Played", f"{heavy_user['total_games']}판", delta="Heavy User")

    st.markdown("---")

    col_left, col_right = st.columns([1.2, 1]) 

    with col_left:
        st.subheader("📌 점수와 승률의 상관관계 (Correlation)")

        fig_scatter = px.scatter(
            filtered_df, 
            x="leaguePoints", 
            y="win_rate", 
            size="total_games", 
            color="win_rate",
            color_continuous_scale="RdBu", 
            hover_data=["puuid"], 
            template="simple_white"
        )
        fig_scatter.update_layout(height=400, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_right:
        st.subheader("🌊 점수 구간별 분포 (Distribution)")
        fig_hist = px.histogram(
            filtered_df, 
            x="leaguePoints", 
            nbins=20, 
            color_discrete_sequence=['#2C3E50'], 
            template="simple_white"
        )
        fig_hist.update_layout(height=400, margin=dict(l=20, r=20, t=30, b=20), showlegend=False)
        st.plotly_chart(fig_hist, use_container_width=True)

    st.subheader("🏆 Top Ranker Leaderboard")

    display_df = filtered_df[['puuid', 'leaguePoints', 'wins', 'losses', 'win_rate', 'total_games']].copy()
    display_df = display_df.sort_values(by='leaguePoints', ascending=False).head(20) # Top 20만

    display_df.columns = ['PUUID (고유ID)', '리그 포인트(LP)', '승리', '패배', '승률(%)', '총 게임 수']

    st.dataframe(
        display_df.style.background_gradient(subset=['승률(%)'], cmap='Blues'),
        use_container_width=True,
        hide_index=True
    )

else:
    st.error("❌ 처리된 데이터 파일이 없습니다. (ETL 파이프라인 미실행)")
    st.warning("터미널에서 `python main.py`를 실행하여 데이터를 수집해주세요.")