import pandas as pd
import streamlit as st

# Load weather data
CSV_PATH = "dags/weather.csv"  # adjust path if needed
df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])

# Streamlit UI
st.set_page_config(page_title="Weather Dashboard", layout="wide")
st.title("🌦️ Weather Dashboard - Kathmandu")

# Latest weather info
latest = df.sort_values("timestamp", ascending=False).iloc[0]
st.metric(label="🌡️ Temperature (°C)", value=f"{latest['temperature']}°C")
st.metric(label="📋 Description", value=latest['description'].capitalize())
st.metric(label="⏰ Timestamp", value=str(latest['timestamp']))

# Display full data
st.subheader("📄 Full Weather Data")
st.dataframe(df.sort_values("timestamp", ascending=False), use_container_width=True)

# Line chart
st.subheader("📈 Temperature Over Time")
st.line_chart(df.set_index("timestamp")["temperature"])
