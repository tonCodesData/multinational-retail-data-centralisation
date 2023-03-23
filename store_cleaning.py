#%%
import pandas as pd


# %%
df = pd.read_csv('stores.csv')
# %%
pd.set_option('display.max_rows', 1000) 
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
# %%


# %% cleaning continent
df['continent'] = df['continent'].astype('string')
# %%
df['continent'] = df['continent'].str.replace('eeEurope', 'Europe')
# %%
df['continent'] = df['continent'].str.replace('eeAmerica', 'America')
# %%
# %%
df = df[(df['continent']=='America') | (df['continent']=='Europe')]
# %%
