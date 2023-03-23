#%%
import pandas as pd
import tabula as tb
import requests as req
import boto3
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
import datetime as dt
import numpy as np

# %%
df = pd.read_csv('card.csv')
# %%
pd.set_option('display.max_rows', 1000) 
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
# %%
# %%

# %% drop null values
df.dropna(inplace=True)


# %% work with card_number 
df['card_number'] = df['card_number'].astype('string')


# %% clean card_provider
df['card_provider'] = df['card_provider'].astype('string')
#%%
card_prov_ls = ['VISA 16 digit', 'JCB 16 digit', 'VISA 13 digit', 'JCB 15 digit', 'VISA 19 digit', 'Diners Club / Carte Blanche', 'American Express', 'Maestro', 'Discover', 'Mastercard']
#%%
df['valid_provider'] = df['card_provider'].apply(lambda x: 'Yes' if x in card_prov_ls else 'No')
#%%
df = df[df['valid_provider']=='Yes']
#%%
df['card_provider'] = df['card_provider'].astype('category')
#%%
df.drop(['valid_provider'], axis=1, inplace=True) 



# %% clean date_payment_confirmed
set(df['date_payment_confirmed'])
# %%
df['date_payment_confirmed'] # just transform into datetime
# %%
df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce', infer_datetime_format=True)
# %%


# %% work with expiry date
set(df['expiry_date']) #some values need to be dropped which are not dates but texts and occurs only once
# %%
df['expiry_date'].value_counts()
# %%
df['dt'] = pd.to_datetime(df['expiry_date'], format="%m/%y")
#%%
df['dt']
# %%
# df['expiry_date'] = df['dt'].map(lambda x: x.strftime('%m/%y'))
# %%
df['expiry_date'] = pd.to_datetime(df['dt']).dt.to_period('M')
# %%
df['expiry_date']
# %%
df.drop(['dt'], axis=1, inplace=True)



# %% working with Unnamed:0 
df.rename(columns={'Unnamed: 0': 'groups'}, inplace=True)