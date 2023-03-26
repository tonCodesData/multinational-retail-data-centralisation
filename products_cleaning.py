#%%
import pandas as pd
df = pd.read_csv('products.csv')

pd.set_option('display.max_rows', 1000) 
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)


# cleaning weight
df.dropna(inplace=True)
df['weights'] = df['weight'].str.replace('([A-Za-z]+)', '')
df['units'] = df['weight'].str.extract('([A-Za-z]+)')

dfx = df[df['units']=='x']
dfx['units'] = 'kg'
dfx['w1'] = dfx['weights'].apply(lambda x: x.split(' ')[0])
dfx['w1'] = dfx['w1'].astype('float64')
dfx['w2'] = dfx['weights'].apply(lambda x: x.split(' ')[2])
dfx['w2'] = dfx['w2'].astype('float64')
dfx['weights'] = (dfx['w1']*dfx['w2'])/1000
dfx.drop(['w1', 'w2'], axis=1, inplace=True)

df['weights'] = df['weights'].str.strip(' ')
df['weights'] = df['weights'].str.rstrip('.')
df = df[df['units'].str.contains('kg|g|ml|oz', regex=True)]
df['weights'] = df['weights'].astype('float64')
df = pd.concat([df, dfx])

weight_conversion = lambda row: row.weights/float(1000) if (row.units=='g' or row.units=='ml') else (row.weights*float(0.0283495) if row.units=='oz' else float(row.weights)) 
df['weights'] = df.apply(weight_conversion, axis=1)
df['units']='kg'
df['weight'] = df['weights']
df.drop(['weights'], axis=1, inplace=True)
df.rename(columns={'weight':'weight_kg'}, inplace=True)
df.drop(['units'], axis=1, inplace=True)

# cleaning other columns
df['product_code'] = df['product_code'].str.upper()
df['date_added'] = pd.to_datetime(df['date_added'], infer_datetime_format=True, errors='coerce')
df['product_code'] = df['product_code'].astype('string')
df['removed'] = df['removed'].astype('string')
df['uuid'] = df['uuid'].astype('string')
df['ean_len'] = df['EAN'].apply(lambda x: len(x))
df['EAN'] = df['EAN'].astype('int64')
df['category']= df['category'].astype('category')
df['product_price']=df['product_price'].str.strip('£')
df['product_price'] = df['product_price'].astype('float64')
df['product_name'] = df['product_name'].astype('string')