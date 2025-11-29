import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np
import altair as alt

# 页面基础配置（标题 / 图标 / 宽屏）
st.set_page_config(
    page_title="China Index Analytics Dashboard",
    page_icon="📈",
    layout="wide",
)


# 一些全局样式美化：背景、Tab 样式、subtitle 等
def apply_page_style():
    st.markdown(
        """
        <style>
        /* 主背景与文字颜色 */
        .main {
            background: radial-gradient(circle at top, #0f172a 0, #020617 45%, #000000 100%);
            color: #e5e7eb;
        }

        /* 顶部默认 header 去掉底色 */
        header[data-testid="stHeader"] {
            background: rgba(0,0,0,0);
        }

        /* 内容整体内边距稍微紧凑一些 */
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2rem;
        }

        /* 标题 */
        h1 {
            font-weight: 700;
        }

        /* 副标题文案 */
        .subtitle {
            font-size: 0.95rem;
            color: #9ca3af;
            margin-top: 0.25rem;
            margin-bottom: 0.3rem;
        }

        .tagline {
            font-size: 0.85rem;
            color: #6b7280;
            margin-bottom: 0.2rem;
        }

        /* 小 pill 样式，用来展示选中的指数 */
        .pill {
            display: inline-flex;
            align-items: center;
            padding: 0.18rem 0.6rem;
            border-radius: 999px;
            border: 1px solid #374151;
            font-size: 0.75rem;
            margin-right: 0.25rem;
            margin-bottom: 0.25rem;
            background: rgba(15,23,42,0.85);
        }

        /* Tabs 样式美化 */
        .stTabs [role="tablist"] {
            gap: 0.5rem;
        }
        .stTabs [role="tab"] {
            padding: 0.4rem 0.9rem;
            border-radius: 999px;
            border: 1px solid #4b5563;
            background-color: #020617;
            color: #9ca3af;
        }
        .stTabs [aria-selected="true"] {
            background: linear-gradient(90deg,#0ea5e9,#6366f1);
            color: white !important;
            border-color: transparent;
        }

        /* Sidebar 背景 */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg,#020617,#030712);
        }

        /* Sidebar 标题 */
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3 {
            color: #e5e7eb;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


# 代码 -> 指数名字映射
INDEX_NAME_MAP = {
    "1000001": "SSE Composite Index",
    "1000032": "SSE Energy",
    "1000033": "SSE Materials",
    "1000034": "SSE Industrials",
    "1000035": "SSE Consumer Discretionary",
    "1000036": "SSE Consumer Staples",
    "1000037": "SSE Health Care",
    "1000038": "SSE Financials",
    "1000039": "SSE Information Technology",
    "1000040": "SSE Telecommunication Services",
    "1000041": "SSE Utilities",
    "1000042": "SSE Central SOEs",
    "1000688": "SSE STAR 50 Index",
    "2399001": "SZSE Component Index",
    "2399006": "ChiNext Index",
}


# ---------- Snowflake 连接 ----------
@st.cache_resource
def get_connection():
    private_key_path = "keys/rsa_key.p8"
    private_key_passphrase = "MLDS430HW2"
    conn = snowflake.connector.connect(
        user="KOALA",
        authenticator="SNOWFLAKE_JWT",
        private_key_file=private_key_path,
        private_key_file_pwd=private_key_passphrase,
        account="azb79167",
        warehouse="FIVETRAN_WAREHOUSE",
        database="FIVETRAN_DATABASE",
        schema="MLDS430_KOALA_INDEX_RAW",
        role="FIVETRAN_ROLE",
    )
    return conn


# ---------- 常用查询 ----------
@st.cache_data
def load_index_codes(_conn):
    """从 fact 表里动态读出有哪些指数代码。"""
    q = """
        SELECT DISTINCT index_code
        FROM fact_index_daily
        ORDER BY index_code
    """
    df = pd.read_sql(q, _conn)
    return df["INDEX_CODE"].tolist()  # Snowflake 默认大写列名


@st.cache_data
def load_fact_index(_conn, index_codes, start_date, end_date):
    format_codes = ",".join([f"'{c}'" for c in index_codes])

    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")

    # 这里只选 Snowflake 表里真实存在的列
    query = f"""
        SELECT
            trade_date,
            index_code,
            open, high, low, close,
            volume,
            amount,
            date_key
        FROM fact_index_daily
        WHERE index_code IN ({format_codes})
          AND trade_date BETWEEN '{start_str}' AND '{end_str}'
        ORDER BY index_code, trade_date
    """
    df = pd.read_sql(query, _conn)

    # 列名统一成小写
    df.columns = [c.lower() for c in df.columns]
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # ===== 在 pandas 里自己算指标 =====
    # 按 index_code + 日期排序
    df = df.sort_values(["index_code", "trade_date"])

    # 每日收益率：当日收盘 / 昨日收盘 - 1
    df["daily_return"] = (
        df.groupby("index_code")["close"]
        .pct_change()
        .fillna(0.0)
    )

    # 累计收益：(1 + r) 连乘 - 1
    df["cumulative_return"] = (
        (1 + df["daily_return"])
        .groupby(df["index_code"])
        .cumprod()
        - 1
    )

    # 最大回撤：从历史高点往下跌多少
    df["cum_max"] = (
        df.groupby("index_code")["cumulative_return"]
        .cummax()
    )
    df["drawdown"] = df["cumulative_return"] / df["cum_max"] - 1

    # 20日滚动波动率
    df["rolling_20d_vol"] = (
        df.groupby("index_code")["daily_return"]
        .rolling(20)
        .std()
        .reset_index(level=0, drop=True)
    )

    # 20日滚动平均收益率（目前没画，用得上就留着）
    df["rolling_20d_avg_return"] = (
        df.groupby("index_code")["daily_return"]
        .rolling(20)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # 代码 -> 名字
    def code_to_name(code: str) -> str:
        code_str = str(code)
        return INDEX_NAME_MAP.get(code_str, code_str)

    df["index_name"] = df["index_code"].astype(str).map(code_to_name)

    # 图里用名字做 legend
    df["index_label"] = df["index_name"]

    return df


# ---------- 主应用 ----------
def main():
    apply_page_style()

    # 顶部标题区
    st.title("China Index Analytics Dashboard")
    st.markdown(
        """
        <div class="subtitle">
            Multi-index analytics for major Chinese equity indices: levels, cumulative return,
            drawdown, volatility and basic seasonality.
        </div>
        <div class="tagline">
            💡 Use the filters on the left to change indices and date range. All charts will update automatically.
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("---")

    conn = get_connection()

    # 动态读取指数列表
    all_indices = load_index_codes(conn)
    if not all_indices:
        st.error("fact_index_daily 中没有任何 index_code，请先确认 dbt 是否成功写表。")
        return

    # Sidebar filters
    st.sidebar.header("📊 Dashboard Filters")

    st.sidebar.markdown("### Index Selection")
    # 在侧边栏显示“代码 - 名字”
    selected_indices = st.sidebar.multiselect(
        "Select index",
        all_indices,
        default=all_indices[:2],  # 默认选前两个
        format_func=lambda code: f"{code} - {INDEX_NAME_MAP.get(str(code), '')}",
    )

    st.sidebar.markdown("### Date Range")
    # 数据范围：2019–2023
    start_date = st.sidebar.date_input(
        "Start date",
        value=pd.to_datetime("2019-01-01")
    )
    end_date = st.sidebar.date_input(
        "End date",
        value=pd.to_datetime("2023-12-31")
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "ℹ️ **Note**: Data sourced from `fact_index_daily` table in Snowflake."
    )

    if not selected_indices:
        st.warning("Please select at least one index.")
        return

    df = load_fact_index(conn, selected_indices, start_date, end_date)

    if df.empty:
        st.warning("No data for selected filters.")
        return

    # 再统一一次列名（保险）
    df.columns = [c.lower() for c in df.columns]

    # 当前选中指数在主区域用 pill 展示一下
    with st.container():
        st.markdown("**Selected indices**")
        pills_html = ""
        for code in selected_indices:
            name = INDEX_NAME_MAP.get(str(code), str(code))
            pills_html += f"<span class='pill'>📈 {name}</span>"
        st.markdown(pills_html, unsafe_allow_html=True)

    st.markdown("")  # 一点间距

    tab1, tab2 = st.tabs(["Performance & Drawdown", "Volatility & Seasonality"])

    with tab1:
        show_performance_tab(df)

    with tab2:
        show_volatility_tab(df)


# ---------- Tab 1：表现 & 累计收益 ----------
def show_performance_tab(df: pd.DataFrame):
    st.markdown(
        "##### 📈 Overall Index Performance"
    )
    st.caption("Index closing levels over time for the selected indices.")
    price_pivot = df.pivot(index="trade_date", columns="index_label", values="close")
    st.line_chart(price_pivot)

    st.markdown("##### 🚀 Cumulative Return")
    st.caption("Cumulative return since the first date in the selected range.")
    cum_pivot = df.pivot(index="trade_date", columns="index_label", values="cumulative_return")
    st.line_chart(cum_pivot)

    st.markdown("##### 📉 Drawdown from Peak")
    dd_pivot = df.pivot(index="trade_date", columns="index_label", values="drawdown")
    st.line_chart(dd_pivot)
    st.caption("Drawdown = Current cumulative return vs. historical peak (per index).")


# ---------- Tab 2：波动率 & 极端日 & 季节性 ----------
def show_volatility_tab(df: pd.DataFrame):
    # 1) 20 日滚动波动率：calm vs turbulent
    st.markdown("##### 🌪 20-day Rolling Volatility")
    st.caption("Standard deviation of daily returns over a 20-day rolling window.")
    vol_pivot = df.pivot(index="trade_date", columns="index_label", values="rolling_20d_vol")
    st.line_chart(vol_pivot)

    # 2) Top 10 大涨 / 大跌日（上下两张表）
    st.markdown("##### 🔝 Top 10 Up & Down Days")

    top_up = df.sort_values("daily_return", ascending=False).head(10)
    top_down = df.sort_values("daily_return", ascending=True).head(10)

    top_up["year"] = top_up["trade_date"].dt.year
    top_up["month"] = top_up["trade_date"].dt.month
    top_down["year"] = top_down["trade_date"].dt.year
    top_down["month"] = top_down["trade_date"].dt.month

    st.markdown("**Top 10 Up Days**")
    st.dataframe(
        top_up[
            [
                "trade_date",
                "year",
                "month",
                "index_code",
                "index_name",
                "daily_return",
                "close",
            ]
        ]
        .assign(daily_return=lambda x: (x["daily_return"] * 100).round(2))
        .rename(columns={"daily_return": "daily_return_%"}),
        use_container_width=True,
    )

    st.markdown("**Top 10 Down Days**")
    st.dataframe(
        top_down[
            [
                "trade_date",
                "year",
                "month",
                "index_code",
                "index_name",
                "daily_return",
                "close",
            ]
        ]
        .assign(daily_return=lambda x: (x["daily_return"] * 100).round(2))
        .rename(columns={"daily_return": "daily_return_%"}),
        use_container_width=True,
    )

    # 3) 日收益率分布 —— 横坐标按数值从负到正
    st.markdown("##### 📊 Distribution of Daily Returns")

    returns_pct = df["daily_return"] * 100
    counts, bin_edges = np.histogram(returns_pct, bins=30)
    bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])
    bin_centers_rounded = np.round(bin_centers, 2)

    hist_df = (
        pd.DataFrame(
            {
                "return_bin": bin_centers_rounded,
                "count": counts,
            }
        )
        .sort_values("return_bin")  # 从最小负值到最大正值
        .set_index("return_bin")
    )
    st.bar_chart(hist_df)

    # 4) Seasonality by Month
    st.markdown("##### 📅 Seasonality by Month (Average Daily Return)")

    df["month"] = df["trade_date"].dt.month
    month_ret = (
        df.groupby(["index_label", "month"])["daily_return"]
        .mean()
        .reset_index()
    )
    month_ret["daily_return_pct"] = month_ret["daily_return"] * 100

    month_pivot = month_ret.pivot(
        index="month", columns="index_label", values="daily_return_pct"
    )
    st.bar_chart(month_pivot)

    # 5) Seasonality by Weekday
    st.markdown("##### 🗓 Seasonality by Weekday (Average Daily Return)")

    # 使用 weekday_num 确定顺序，再映射成 Mon–Fri，并用 Altair 显式指定排序
    df["weekday_num"] = df["trade_date"].dt.weekday  # Monday=0
    weekday_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}

    weekday_ret = (
        df.groupby(["index_label", "weekday_num"])["daily_return"]
        .mean()
        .reset_index()
    )
    weekday_ret["weekday_name"] = weekday_ret["weekday_num"].map(weekday_map)
    weekday_ret["daily_return_pct"] = weekday_ret["daily_return"] * 100

    weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri"]

    chart = (
        alt.Chart(weekday_ret)
        .mark_bar()
        .encode(
            x=alt.X("weekday_name:N", sort=weekday_order, title="Weekday"),
            y=alt.Y("daily_return_pct:Q", title="Average daily return (%)"),
            color=alt.Color("index_label:N", title="Index"),
            tooltip=[
                alt.Tooltip("index_label:N", title="Index"),
                alt.Tooltip("weekday_name:N", title="Weekday"),
                alt.Tooltip("daily_return_pct:Q", title="Avg return (%)", format=".2f"),
            ],
        )
        .properties(height=300)
    )

    st.altair_chart(chart, use_container_width=True)


if __name__ == "__main__":
    main()
