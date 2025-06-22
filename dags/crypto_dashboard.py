import pandas as pd
import streamlit as st

# Load the data
CSV_PATH = "dags/crypto_prices.csv"  # adjust if needed
df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])

st.set_page_config(page_title="Crypto Price Tracker", layout="wide")
st.title("📈 Crypto Price Tracker")

# Show table
st.subheader("📋 Latest Crypto Prices")
st.dataframe(df)

# Bar chart for current prices
st.subheader("💰 Current Price (USD)")
st.bar_chart(df.set_index("coin")["price_usd"])

# Bar chart for market caps
st.subheader("🏦 Market Capitalization (USD)")
st.bar_chart(df.set_index("coin")["market_cap"])

