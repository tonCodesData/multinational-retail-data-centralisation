#%%
from sqlalchemy import create_engine
import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import tabula as tb
import requests as req
import boto3
#%%
class DataExtractor:
    # This class will work as a utility class, in it you will be creating methods 
    # that help extract data from different data sources.
    # The methods contained will be fit to extract data from a particular data source, 
    # these sources will include CSV files, an API and an S3 bucket.
    def __init__(self) -> None:
        pass
    #step 5: Develop a method called read_rds_table 
    # which will extract the database table to a pandas DataFrame.
    # take in an instance of DatabaseConnector class 
    # and the table name as an argument and return a pandas Df
    def read_rds_table(self, table_name, engine):
        users = pd.read_sql_query(f"select * from {table_name}", engine)
        return users
    
    #returns df.
    #extract all pages from the pdf using tabular-py python package, and return df
    def retrieve_pdf_data(self, link):
        df = tb.read_pdf(link, stream=True, pages = 'all') #jana lagbe
        return df
    
    #returns number of stores to extract
    def list_number_of_stores(self, num_stores_endpoint, header_dict):
        response = req.get(num_stores_endpoint, headers = header_dict)
        json_data = response.json()
        return json_data['number_stores']        

    #extracts all stores and save them in pandas df
    def retrieve_stores_data(self, num_stores_endpoint, header_dict):
        response = req.get(num_stores_endpoint, headers = header_dict)
        data = response.json()
        return pd.DataFrame(data)

    def retrieve_stores_data(self, retrieve_store_endpoint, number_of_stores, header_dict):
        stores_ls = []
        for store_number in range(number_of_stores):
            response = req.get(retrieve_store_endpoint.format(store_number = store_number), headers = header_dict)
            data = response.json()
            stores_ls.append(data)
        stores_df = pd.DataFrame(stores_ls)
        return stores_df
    
    def extract_from_s3(self, s3_address): 
        s3 = boto3.client('s3')
        bucket_name = s3_address.split('/')[2]
        file_path_n_name = '/'.join(s3_address.split('/')[3:])
        with open('products_need_claning.csv', 'wb') as prod_csv:
            s3.download_fileobj(bucket_name, file_path_n_name, prod_csv)
        products_data = pd.read_csv('products_need_claning.csv')
        products_df = pd.DataFrame(products_data)
        return products_df

#-------------------------------------------------------------------------------
# execution

#%%
db_connector = DatabaseConnector()
#%%
data_extractor = DataExtractor()
#%%
data_cleaner = DataCleaning()
#%%
db_creds = db_connector.read_db_creds()
engine = db_connector.init_db_engine(db_creds)
#%%
table_name_ls = db_connector.list_db_tables(engine)
for i in range (len(table_name_ls)): 
    print (table_name_ls[i])

# Use the read_rds_table method
# to extract the table containing user data 
# and return a pandas DataFrame.
users = data_extractor.read_rds_table('legacy_users', engine)
users_cleaned = data_cleaner.clean_user_data(users)

#upload to Sales_data
db_connector.upload_to_db(users_cleaned, 'dim_users', db_connector.init_local_db_engine())

#%%
#importing from pdf and transforming to dfs
card_df = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
#%%
print(len(card_df))
card_df_page_1 = card_df[0]
#%%
card_cleaned = data_cleaner.clean_card_data(card_df_page_1)
#%%
db_connector.upload_to_db(card_cleaned, 'dim_card_details', db_connector.init_local_db_engine())

#%%
#M2T5: extract from api and clean details of each store, and upload
header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
# %%
number_of_stores = data_extractor.list_number_of_stores(num_stores_endpoint, header_dict)
# extracts all stores and save them in pandas df
stores_df = data_extractor.retrieve_stores_data(retrieve_store_endpoint, number_of_stores, header_dict)
# %%
stores_df.head(10)
# %%
store_data_cleaned = data_cleaner.clean_store_data(stores_df)
#%%
db_connector.upload_to_db(store_data_cleaned, 'dim_store_details', db_connector.init_local_db_engine())

#%% --------------------------------------------------------
# downloading products csv from s3 bucket
s3_address = 's3://data-handling-public/products.csv'
products_df = data_extractor.extract_from_s3(s3_address)

# make uniform weight, clean, and upload to sales data
uniform_prod_weight_unit_df = data_cleaner.convert_product_weights(products_df)
cleaned_prod_df = data_cleaner.clean_products_data(uniform_prod_weight_unit_df)
db_connector.upload_to_db(cleaned_prod_df, 'dim_products', db_connector.init_local_db_engine())
# %%
