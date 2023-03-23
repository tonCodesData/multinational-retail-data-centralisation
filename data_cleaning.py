import pandas as pd

class DataCleaning:
    # clean data of each data sources
    def __init__(self) -> None:
        pass
    def clean_user_data(self, df):         
        # sort_values
        df.sort_values(by=['index'], inplace=True)

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

        # standardizing address
        df = new
        return df                                                                                       
    

    # hod called clean_card_data in your DataCleaning class 
    # to clean the data to remove any erroneous values, 
    # NULL values or errors with formatting.
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
        df['dt'] = pd.to_datetime(df['expiry_date'], format="%m/%y")
        # df['expiry_date'] = df['dt'].map(lambda x: x.strftime('%m/%y'))
        df['expiry_date'] = pd.to_datetime(df['dt']).dt.to_period('M')
        df.drop(['dt'], axis=1, inplace=True)

        # working with Unnamed:0 
        df.rename(columns={'Unnamed: 0': 'groups'}, inplace=True)
        return df
    
    def clean_store_data(self, df): 
        cleaned_df = df.dropna()
        # errors in dates, 
        # incorrectly typed values
        # rows filled with wrong information
        return cleaned_df
    
    def convert_product_weights(self, products_df):
        uniform_prod_weight_unit_df = products_df
        return uniform_prod_weight_unit_df

    def clean_products_data(self, uniform_prod_weight_unit_df): 
        cleaned_prod_df = uniform_prod_weight_unit_df.dropna()
        return cleaned_prod_df
    
    #m2t7
    #step3: 
    def clean_orders_data(self, orders_df):
        # You should remove the columns, first_name, last_name 
        # and 1 to have the table in the correct form before uploading to the database.
        # You will see that the orders data contains column headers 
        # which are the same in other tables.
        # This table will act as the source of truth for your sales data 
        # and will be at the center of your star based database schema.
        cleaned_orders_df = orders_df
        return cleaned_orders_df

    #m2t8 
    def clean_events_date(self, date_events_df):
        cleaned_date_events_df = date_events_df
        return cleaned_date_events_df