import os
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

from utils.config import config

# -----------------------------------------------------------------------------
# 1. Application Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Challenger League Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)


# -----------------------------------------------------------------------------
# 2. Data Access Layer
# -----------------------------------------------------------------------------
@st.cache_data
def load_dataset() -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Load and preprocess the processed dataset from the local file system.

    Decorators:
        @st.cache_data: Caches the result to optimize performance on re-runs.
                        Cache is invalidated when the underlying file changes.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]:
            - Preprocessed DataFrame (None if file missing).
            - Last modification timestamp string (None if file missing).
    """
    data_path = config["path"]["processed_data"]

    if not os.path.exists(data_path):
        return None, None

    # Metadata: File modification time
    mod_time = os.path.getmtime(data_path)
    last_updated = datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d %H:%M:%S")

    # Load Data
    df = pd.read_csv(data_path)

    # Feature Engineering for Dashboard
    # Calculation: Win Rate (%) and Total Games
    df["win_rate"] = (df["wins"] / (df["wins"] + df["losses"]) * 100).round(1)
    df["total_games"] = df["wins"] + df["losses"]

    # Data Cleaning: Remove outliers where win rate > 100%
    df = df[df["win_rate"] <= 100]

    return df, last_updated


# Load data into memory
df, last_updated = load_dataset()

# -----------------------------------------------------------------------------
# 3. Sidebar: Global Filters & Metadata
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("Dashboard Controls")

    if df is not None:
        st.subheader("Filter Settings")

        # LP Filter
        min_lp = int(df["leaguePoints"].min())
        max_lp = int(df["leaguePoints"].max())
        target_lp = st.slider("Minimum League Points", min_lp, max_lp, min_lp)

        # Apply Filter
        filtered_df = df[df["leaguePoints"] >= target_lp]

        st.markdown("---")
        st.markdown(f"**Selected Users:** {len(filtered_df):,}")
    else:
        filtered_df = None
        st.warning("Data source unavailable.")

    st.markdown("---")
    st.caption(f"System Version: 1.0.0\nData Source: Riot League-V4 API")

# -----------------------------------------------------------------------------
# 4. Main Dashboard Layout
# -----------------------------------------------------------------------------
st.title("League of Legends: Challenger Tier Analytics")
st.markdown("Performance metrics and correlation analysis for top-tier players.")

if df is not None:
    # 4.1. System Status Indicator
    st.markdown(
        f"""
    <div style='background-color: #f8f9fa; padding: 12px; border-radius: 4px; border-left: 5px solid #28a745; margin-bottom: 20px;'>
        <small><strong>STATUS: OPERATIONAL</strong> | Last Updated: {last_updated} | Total Records: {len(df):,}</small>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # 4.2. Key Performance Indicators (KPIs)
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    with kpi1:
        st.metric(label="Analyzed Players", value=f"{len(filtered_df):,}")
    with kpi2:
        avg_lp = int(filtered_df["leaguePoints"].mean())
        st.metric(label="Average LP", value=f"{avg_lp:,}")
    with kpi3:
        avg_wr = filtered_df["win_rate"].mean()
        st.metric(label="Average Win Rate", value=f"{avg_wr:.1f}%")
    with kpi4:
        max_games = filtered_df["total_games"].max()
        st.metric(label="Max Games Played", value=f"{max_games}")

    st.divider()

    # 4.3. Charts Section
    col_chart_1, col_chart_2 = st.columns([1.5, 1])

    with col_chart_1:
        st.subheader("Correlation: LP vs. Win Rate")
        # Scatter Plot: Visualizes relationship between ranking score and win efficiency
        fig_scatter = px.scatter(
            filtered_df,
            x="leaguePoints",
            y="win_rate",
            size="total_games",
            color="win_rate",
            color_continuous_scale="Viridis",  # Professional color scale
            opacity=0.7,
            labels={"leaguePoints": "League Points (LP)", "win_rate": "Win Rate (%)"},
            template="plotly_white",  # Clean background
        )
        fig_scatter.update_layout(height=400, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_chart_2:
        st.subheader("LP Distribution")
        # Histogram: Shows the density of players across LP ranges
        fig_hist = px.histogram(
            filtered_df,
            x="leaguePoints",
            nbins=20,
            color_discrete_sequence=["#34495e"],  # Corporate Navy Blue
            labels={"leaguePoints": "League Points (LP)"},
            template="plotly_white",
        )
        fig_hist.update_layout(
            height=400, margin=dict(l=20, r=20, t=30, b=20), showlegend=False
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    # 4.4. Detailed Data Table
    st.subheader("Top Performers Leaderboard")

    # Prepare display dataframe
    display_cols = ["leaguePoints", "wins", "losses", "win_rate", "total_games"]
    display_df = (
        filtered_df[display_cols]
        .sort_values(by="leaguePoints", ascending=False)
        .head(50)
    )

    # Render interactive table
    st.dataframe(
        display_df.style.background_gradient(subset=["win_rate"], cmap="GnBu"),
        use_container_width=True,
        height=400,
    )

else:
    # Error State Handling
    st.error("Data Pipeline Error: Processed data file not found.")
    st.markdown(
        """
        **Troubleshooting:**
        1. Verify that the ETL pipeline (`main.py`) has been executed successfully.
        2. Check if `data/processed/cleaned_data.csv` exists.
        3. Review `logs/etl.log` for any upstream errors.
    """
    )
