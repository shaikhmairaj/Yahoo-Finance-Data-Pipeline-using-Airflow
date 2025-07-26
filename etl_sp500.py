import requests
import pandas as pd
from bs4 import BeautifulSoup
import random
from datetime import datetime
import os

def fetch_transform_load():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', {'id': 'constituents'})
    df = pd.read_html(str(table))[0]

    df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)

    # Pick two specific companies (Apple and Nvidia)
    df = df[df['Symbol'].isin(['AAPL', 'NVDA'])]

    # Simulate stock data
    df['trade_date'] = datetime.now()
    df['close_price'] = [round(random.uniform(100, 500), 2) for _ in range(len(df))]
    df['high_price'] = df['close_price'] + random.uniform(1, 10)
    df['low_price'] = df['close_price'] - random.uniform(1, 10)
    df['open_price'] = df['close_price'] + random.uniform(-5, 5)
    df['volume'] = [random.randint(1000000, 5000000) for _ in range(len(df))]
    df['symbol'] = df['Symbol']
    df['close_change'] = [round(random.uniform(-5, 5), 2) for _ in range(len(df))]
    df['close_pct_change'] = round((df['close_change'] / df['close_price']) * 100, 2)

    final_df = df[[
        'trade_date', 'close_price', 'high_price', 'low_price',
        'open_price', 'volume', 'symbol', 'close_change', 'close_pct_change'
    ]]

    os.makedirs("data", exist_ok=True)
    final_df.to_csv("data/stock_prices.csv", index=False)
