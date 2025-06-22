from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import logging

# === API Settings ===
COINS = ['bitcoin', 'ethereum','dogecoin', 'litecoin', 'ripple']
CURRENCY = 'usd'
API_URL = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(COINS)}&vs_currencies={CURRENCY}&include_market_cap=true"

CSV_PATH = '/opt/airflow/dags/crypto_prices.csv'

def fetch_and_save_crypto_data():
    response = requests.get(API_URL)
    data = response.json()

    records = []
    for coin in COINS:
        coin_data = data.get(coin, {})
        records.append({
            'coin': coin,
            'price_usd': coin_data.get('usd'),
            'market_cap': coin_data.get('usd_market_cap'),
            'timestamp': datetime.now()
        })

    df = pd.DataFrame(records)
    df.to_csv(CSV_PATH, index=False)

    logging.info("✅ Crypto data saved!")
    logging.info(df)

with DAG(
    dag_id="crypto_price_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["crypto", "etl"]
) as dag:

    fetch_crypto_task = PythonOperator(
        task_id="fetch_and_save_crypto_data",
        python_callable=fetch_and_save_crypto_data
    )
