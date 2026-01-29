import streamlit as st
import duckdb
import pandas as pd
from src.config import DB_PATH

st.set_page_config(page_title="Security Login Analytics", layout="wide")
st.title("Security Login Analytics Dashboard")

con = duckdb.connect(str(DB_PATH), read_only=True)

kpis = con.execute("SELECT * FROM analytics.mart_daily_kpis").df()
seg = con.execute("SELECT * FROM analytics.mart_risk_segments").df()

st.subheader("Daily KPIs")
st.line_chart(kpis.set_index("date")[["total_events","suspicious_rate","success_rate"]])

st.subheader("Risk Segments")
st.dataframe(seg)

con.close()