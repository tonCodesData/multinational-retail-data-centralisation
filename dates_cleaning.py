#%%
import pandas as pd
#%%
url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
df = pd.read_json(url)
# %%
pd.set_option('display.max_rows', 1000) 
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
#%%
df['date'] = df['year'] + '-' + df['month'] +'-' + df['day']
#%%
df['date_time'] = df['date'] + ' ' + df['timestamp']
# %%
df['timestamp'] = pd.to_datetime(df['date_time'], errors='coerce', infer_datetime_format=True)
# %%
df.drop(['year','month','date','day', 'date_time'], axis=1, inplace=True)
# %%
df = df[df['time_period'].apply(lambda x: x in ['Evening', 'Midday', 'Morning', 'Late_Hours'])]
# %%
df['time_period'] = df['time_period'].astype('category')
# %%
df['date_uuid'] = df['date_uuid'].astype('string')
# %%
df.info()
# %%
