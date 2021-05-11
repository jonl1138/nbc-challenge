from datetime import datetime
import numpy as np
import pandas as pd
import requests

# Problem/ Schema 1
with requests.get('https://api.coinranking.com/v1/public/coin/1/history/30d', stream=True) as response:
    response = response.json()
    data = response['data']['history']
df = pd.DataFrame(data)
df['date'] = [datetime.fromtimestamp(x/1000).isoformat() for x in df['timestamp']]
df = df[df['date'].astype(str).str.contains("00:00:00")]
df.reset_index(drop=True, inplace=True)
df['price'] = df['price'].astype(float).round(2)
df['highSinceStart'] = df['price'].cummax()
df.loc[df.duplicated(subset=['highSinceStart']), 'highSinceStart'] = np.nan
df['highSinceStart'] = df.apply(lambda x: True if x['price'] == x['highSinceStart'] else False, axis=1)
df.loc[0,'highSinceStart'] = np.nan
df['change'] = df['price'].diff()
df['direction'] = df.apply(lambda x: 'up' if x['change'] > 0 else ('down' if x['change'] < 0 else (np.nan if str(x['change']) == 'nan' else 'same')), axis=1)
df['dayOfWeek'] = [datetime.strptime(x, '%Y-%m-%dT%H:%M:%S').strftime('%A') for x in df['date']]
df.drop(columns=['timestamp'], inplace=True)
df.to_json('problem1.json', orient='records')

# Problem/ Schema 2
with requests.get('https://api.coinranking.com/v1/public/coin/1/history/30d', stream=True) as response:
    response = response.json()
    data = response['data']['history']
df = pd.DataFrame(data)
df['date'] = [datetime.fromtimestamp(x/1000).isoformat() for x in df['timestamp']]
df['date'] = [x.split('T')[0] + 'T00:00:00' for x in df['date']]
df['price'] = df['price'].astype(float).round(2)
volatilityAlert_list = []
for i, row in df.iterrows():
    z_score = (row['price'] - df[df['date'] == row['date']]['price']).mean()/df[df['date'] == row['date']]['price'].std(ddof=0)
    if abs(z_score) > 2:
        volatilityAlert_list.append(True)
    else:
        volatilityAlert_list.append(False)
df['volatilityAlert'] = volatilityAlert_list
prices = df.drop_duplicates(subset=['date'])['price']
df = df[['date', 'volatilityAlert', 'price']].groupby('date').agg({'price': ['mean', 'var'], 'volatilityAlert': ['any']}, as_index=False).reset_index()
df.columns = ['_'.join(col).strip() for col in df.columns.values]
df.rename(columns={'date_': 'date', 'price_mean': 'dailyAverage', 'price_var': 'dailyVariance', 'volatilityAlert_any': 'volatilityAlert'}, inplace=True)
df['price'] = prices.values
df.to_json('problem2.json', orient='records')