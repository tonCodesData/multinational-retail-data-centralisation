#%%
import pandas as pd
df = pd.read_csv('stores.csv')
#%%
pd.set_option('display.max_rows', 1000) 
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)

# %% cleaning 'lat'
df = df[df['lat'].isnull()]
df = df.drop(['lat'], axis=1)

#%% dropping null values
df.dropna(inplace=True)

# %% cleaning continent
df['continent'] = df['continent'].replace({'eeEurope':'Europe', 'eeAmerica':'America'})
df['continent'] = df['continent'].astype('category')

# %% cleaning latitude
df['latitude']= df['latitude'].astype('float64')
df['latitude'] = df['latitude'].round(4)

# %% cleaning country code
df['country_code'] = df['country_code'].astype('category')

# %% cleaning store type
df['store_type'] = df['store_type'].astype('category')

# %% cleaning opening date
df['opening_date'] = pd.to_datetime(df['opening_date'], infer_datetime_format=True, errors='coerce')

# %% cleaning staff members
df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce').astype('Int64')

# %% cleaning store_code
df['store_code'] = df['store_code'].astype('string')

# %% cleaning locality
df['locality'] = df['locality'].astype('string')

# %% cleaning longitude
df['longitude']= df['longitude'].astype('float64')
df['longitude'] = df['longitude'].round(4)



#%%clening address
ukaddress = df[df['country_code']=='GB']
# %%
ukaddress['address'] = ukaddress['address'].apply(lambda x: x.split(',')[0])
# %%
ukaddress['post_code'] = ukaddress['address'].apply(lambda x: x.split('\n').pop(-1))
# %%
ukaddress['city'] = ukaddress['address'].apply(lambda x: x.split('\n').pop(-2))
# %%
ukaddress['house_road'] = ukaddress['address'].apply(lambda x: x.split('\n')[0]+ ', ' +x.split('\n').pop(1))
# %%
ukaddress['house_road'] = ukaddress['house_road'].str.title()
# %%
ukaddress.drop(['address'], axis=1, inplace=True)


#%%
usaddress = df[df['country_code']=='US']
# %%
usaddress['house_road'] = usaddress['address'].apply(lambda x: x.split('\n').pop(0))
# %%
usaddress['city_zip_local'] = usaddress['address'].apply(lambda x: x.split('\n').pop(-1))
# %%
usaddress['city'] = usaddress['city_zip_local'].apply(lambda x: x.split(',')[0])
#%%
usaddress['zip'] = usaddress['city_zip_local'].apply(lambda x: x.split(',')[1])
# %%
usaddress['house_road'] = usaddress['house_road'].str.title()
# %%
usaddress.drop(['city_zip_local'], axis=1, inplace=True)
# %%
usaddress.rename(columns={'zip':'post_code'}, inplace=True)
#%%
usaddress.drop(['address'], axis=1, inplace=True)


# %%
deaddress = df[df['country_code']=='DE']
#%%
deaddress['house_road'] = deaddress['address'].apply(lambda x: x.split('\n').pop(0))
# %%
deaddress['city_zip_local'] = deaddress['address'].apply(lambda x: x.split('\n').pop(-1))
# %%
deaddress['city'] = deaddress['city_zip_local'].apply(lambda x: x.split(',')[0].split(' ')[-1])
#%%
deaddress['post_code'] = deaddress['city_zip_local'].apply(lambda x: x.split(',')[0].split(' ')[0])
# %%
deaddress.drop(['city_zip_local'], axis=1, inplace=True)
#%%
deaddress.drop(['address'], axis=1, inplace=True)


# %%
df = pd.concat([ukaddress, usaddress, deaddress])


# %%
df.info()
# %%
df['Unnamed: 0'].value_counts()
# %%
df.head()