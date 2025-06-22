from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os
import logging

# === Configuration ===
CITY = "Kathmandu"
API_KEY = "066fbe86bfe0272757daeef9d98e054f"
BASE_URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

def fetch_weather(**context):
    response = requests.get(BASE_URL)
    data = response.json()

    weather_data = {
        "city": CITY,
        "temperature": data["main"]["temp"],
        "description": data["weather"][0]["description"],
        "timestamp": str(datetime.now())  # convert to string for XCom
    }

    df = pd.DataFrame([weather_data])
    df.to_csv('/opt/airflow/dags/weather.csv', index=False)
    print("✅ Weather data saved!")
    logging.info("✅ Weather data saved!")
    logging.info(f"🌤️ Weather data: {weather_data}")  # This goes in logs too
    return weather_data  # 🔁 This will be saved in XCom
    


with DAG(
    dag_id="weather_etl",
    start_date=datetime(2025, 5, 8),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "etl"]
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather,
        provide_context=True
    )
