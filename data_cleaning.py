import pandas as pd
import numpy as np

# clean data of each data sources
class DataCleaning:
    def __init__(self) -> None:
        pass

    def clean_orders_data(self, df):
        df.drop(['level_0', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        df['product_code'] = df['product_code'].str.upper()
        return df
    
    def clean_card_data(self, df): 
        # drop null values
        df.dropna(inplace=True)

        # work with card_number 
        df['card_number'] = df['card_number'].astype('string')

        # clean card_provider
        df['card_provider'] = df['card_provider'].astype('string')
        card_prov_ls = ['VISA 16 digit', 'JCB 16 digit', 'VISA 13 digit', 'JCB 15 digit', 'VISA 19 digit', 'Diners Club / Carte Blanche', 'American Express', 'Maestro', 'Discover', 'Mastercard']
        df['valid_provider'] = df['card_provider'].apply(lambda x: 'Yes' if x in card_prov_ls else 'No')
        df = df[df['valid_provider']=='Yes']
        df['card_provider'] = df['card_provider'].astype('category')
        df.drop(['valid_provider'], axis=1, inplace=True) 

        # clean date_payment_confirmed
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce', infer_datetime_format=True)

        # work with expiry date
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format="%m/%y")
        # df['expiry_date'] = df['dt'].map(lambda x: x.strftime('%m/%y'))
        #df['expiry_date'] = pd.to_datetime(df['expiry_date']).dt.to_period('M')
        #df.drop(['dt'], axis=1, inplace=True)

        return df    

    def clean_store_data(self, df):         
        # cleaning 'lat'
        df.drop(columns = ['lat'], inplace=True)

        # cleaning continent
        df['continent'] = df['continent'].replace({'eeEurope':'Europe', 'eeAmerica':'America'})
        df['continent'] = df['continent'].astype('category')
        df = df[df['continent'].apply(lambda x: x in ['America', 'Europe'])]
        # # cleaning latitude
        df['latitude']= df['latitude'].astype('float64')
        df['latitude'] = df['latitude'].round(4)

        # cleaning country code
        df['country_code'] = df['country_code'].astype('category')

        # cleaning store type
        df['store_type'] = df['store_type'].astype('category')

        # cleaning opening date
        df['opening_date'] = pd.to_datetime(df['opening_date'], infer_datetime_format=True, errors='coerce')

        # cleaning staff members
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce').astype('Int64')

        # cleaning store_code
        df['store_code'] = df['store_code'].astype('string')

        # cleaning locality
        df['locality'] = df['locality'].astype('string')

        # # cleaning longitude
        # df['longitude']= df['longitude'].astype('float64', errors='ignore')
        # df['longitude'] = df['longitude'].round(4)

        # # cleaning address
        # ukaddress = df[df['country_code']=='GB']
        # ukaddress['address'] = ukaddress['address'].apply(lambda x: x.split(',')[0])
        # ukaddress['post_code'] = ukaddress['address'].apply(lambda x: x.split('\n').pop(-1))
        # ukaddress['city'] = ukaddress['address'].apply(lambda x: x.split('\n').pop(-2))
        # ukaddress['house_road'] = ukaddress['address'].apply(lambda x: x.split('\n')[0]+ ', ' +x.split('\n').pop(1))
        # ukaddress['house_road'] = ukaddress['house_road'].str.title()
        # ukaddress.drop(['address'], axis=1, inplace=True)

        # usaddress = df[df['country_code']=='US']
        # usaddress['house_road'] = usaddress['address'].apply(lambda x: x.split('\n').pop(0))
        # usaddress['city_zip_local'] = usaddress['address'].apply(lambda x: x.split('\n').pop(-1))
        # usaddress['city'] = usaddress['city_zip_local'].apply(lambda x: x.split(',')[0])
        # usaddress['zip'] = usaddress['city_zip_local'].apply(lambda x: x.split(',')[1])
        # usaddress['house_road'] = usaddress['house_road'].str.title()
        # usaddress.drop(['city_zip_local'], axis=1, inplace=True)
        # usaddress.rename(columns={'zip':'post_code'}, inplace=True)
        # usaddress.drop(['address'], axis=1, inplace=True)

        # deaddress = df[df['country_code']=='DE']
        # deaddress['house_road'] = deaddress['address'].apply(lambda x: x.split('\n').pop(0))
        # deaddress['city_zip_local'] = deaddress['address'].apply(lambda x: x.split('\n').pop(-1))
        # deaddress['city'] = deaddress['city_zip_local'].apply(lambda x: x.split(',')[0].split(' ')[-1])
        # deaddress['post_code'] = deaddress['city_zip_local'].apply(lambda x: x.split(',')[0].split(' ')[0])
        # deaddress.drop(['city_zip_local'], axis=1, inplace=True)
        # deaddress.drop(['address'], axis=1, inplace=True)

        # df = pd.concat([ukaddress, usaddress, deaddress])

        return df


    def clean_user_data(self, df):        
        # look for NULL values
        #%%
        # sort_values
        df.sort_values(by=['index'], inplace=True)
        df.drop(['index'], axis=1, inplace=True)
        #%%
        df.rename(columns={'Unnamed: 0':'id'}, inplace=True)
        #%%
        df.dropna(inplace=True)

        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce', infer_datetime_format=True)
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce', infer_datetime_format=True)

        ## make all names titled
        df['last_name'] = df['last_name'].astype('string')
        df['last_name'] = df['last_name'].apply(lambda x: x.title())
        # rows filled with wrong information
        
        # replace 'GGB' with 'GB'
        # drop other wrong ones
        df['country_code'] = df['country_code'].replace('GGB','GB')
        mask = (df.country_code=='GB') | (df.country_code=='DE') | (df.country_code=='US') 
        new = df[mask]

        # validating and standardizing phone number
        new['phone_number'] = new['phone_number'].astype('string')
        new['phone_number'] = new['phone_number'].str.replace(' ', '')
        new['phone_number'] = new['phone_number'].str.replace('\(', '')
        new['phone_number'] = new['phone_number'].str.replace('\)', '')
        new['phone_number'] = new['phone_number'].str.replace('.', '')
        new['phone_number'] = new['phone_number'].str.replace('+', '00')
        new['phone_number'] = new['phone_number'].str.replace('-', '')
        df = new
        return df           



    def convert_product_weights(self, df):
        # cleaning weight
        df.dropna(inplace=True)
        df['weights'] = df['weight'].str.replace('[^0-9.]', '', regex=True)
        df['units'] = df['weight'].str.extract('([A-Za-z]+)')

        dfx = df[df['units']=='x']
        dfx['units'] = 'kg'
        dfx['w1'] = dfx['weight'].apply(lambda x: x.split(' ')[0])
        dfx['w1'] = dfx['w1'].astype('float64')
        dfx['w2'] = dfx['weight'].apply(lambda x: x.split(' ')[2]).str.replace('[^0-9.]', '', regex=True)
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
        return df

    def clean_products_data(self, df): 
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
        return df
    
    
    
    def clean_events_date(self, df):
        df = df[df['time_period'].apply(lambda x: x in ['Evening', 'Midday', 'Morning', 'Late_Hours'])]
        df['date_uuid'] = df['date_uuid'].astype('string')
        df.info()
        return df
