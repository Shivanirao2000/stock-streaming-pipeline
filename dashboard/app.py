import streamlit as st
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(page_title="Live Stock Tracker", layout="wide")
st.title("📈 Live Stock Tracker")

TICKERS = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"]

@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="stocks", user="admin", password="secret"
    )

@st.cache_data(ttl=2)
def fetch_data():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT ticker, price, rolling_avg, ts
        FROM stock_prices
        ORDER BY ts DESC
        LIMIT 500
    """, conn)
    return df

# Auto-refresh
st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

df = fetch_data()

if df.empty:
    st.warning("No data yet — make sure producer and processor are running.")
    st.stop()

# --- Metrics Row ---
cols = st.columns(5)
for i, ticker in enumerate(TICKERS):
    tdf = df[df["ticker"] == ticker].reset_index(drop=True)
    if tdf.empty:
        continue
    current = tdf.loc[0, "price"]
    prev = tdf.loc[1, "price"] if len(tdf) > 1 else current
    avg = tdf.loc[0, "rolling_avg"]
    delta = round(current - prev, 2)
    cols[i].metric(
        label=ticker,
        value=f"${current:.2f}",
        delta=f"{delta:+.2f}",
        delta_color="normal"
    )
    cols[i].caption(f"5-tick avg: ${avg:.2f}")

st.divider()

# --- Price History Chart ---
st.subheader("Price History (last 50 ticks per ticker)")
fig = go.Figure()
for ticker in TICKERS:
    tdf = df[df["ticker"] == ticker].sort_values("ts").tail(50)
    if tdf.empty:
        continue
    fig.add_trace(go.Scatter(
        x=tdf["ts"], y=tdf["price"],
        mode="lines", name=ticker
    ))

fig.update_layout(
    xaxis_title="Time",
    yaxis_title="Price (USD)",
    legend_title="Ticker",
    height=450,
    margin=dict(l=0, r=0, t=20, b=0)
)
st.plotly_chart(fig, use_container_width=True)

# Auto-rerun every 3 seconds
import time
time.sleep(3)
st.rerun()
