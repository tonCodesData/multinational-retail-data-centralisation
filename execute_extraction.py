#%%
from sqlalchemy import create_engine
import pandas as pd
import tabula as tb
import requests as req
import boto3
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

#-------------------------------------------------------------------------------
# execution
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaner = DataCleaning()


#-------------------------------------------------------------------------------
#%% importing user data from aws rds, cleaning user data and uploading to sales_data
db_creds = db_connector.read_db_creds()
engine = db_connector.init_db_engine(db_creds)
table_name_ls = db_connector.list_db_tables(engine)
user_data = data_extractor.read_rds_table('legacy_users', engine)
user_clean_data = data_cleaner.clean_user_data(user_data)
db_connector.upload_to_db(user_clean_data, 'dim_users', db_connector.init_local_db_engine())


#-------------------------------------------------------------------------------
#%% importing card data from pdf, cleaning card data, and uploading to sales_data
card_df = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
card_cleaned = data_cleaner.clean_card_data(card_df)
db_connector.upload_to_db(card_cleaned, 'dim_card_details', db_connector.init_local_db_engine())

#%% extract from api and clean details of each store, and upload
header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
number_of_stores = data_extractor.list_number_of_stores(num_stores_endpoint, header_dict)
stores_df = data_extractor.retrieve_stores_data(retrieve_store_endpoint, number_of_stores, header_dict)
# %%
store_data_cleaned = data_cleaner.clean_store_data(stores_df)
#%%
db_connector.upload_to_db(store_data_cleaned, 'dim_store_details', db_connector.init_local_db_engine())


#%% --------------------------------------------------------
# M2Task6: Extract and clean product details

# downloading products csv from s3 bucket
s3_address = 's3://data-handling-public/products.csv'
products_df = data_extractor.extract_from_s3(s3_address)

# make uniform weight, clean, and upload to sales data
uniform_prod_weight_unit_df = data_cleaner.convert_product_weights(products_df)
cleaned_prod_df = data_cleaner.clean_products_data(uniform_prod_weight_unit_df)
db_connector.upload_to_db(cleaned_prod_df, 'dim_products', db_connector.init_local_db_engine())


# %%--------------------------------------------------------
# M2Task7: Retrieve and clean orders table from AWS RDS
#%%step2: extract orders_table and convert into pd df
db_creds = db_connector.read_db_creds()
engine = db_connector.init_db_engine(db_creds)
orders_df = data_extractor.read_rds_table('orders_table', engine)
#%%step3: clean the orders_df
cleaned_orders_df = data_cleaner.clean_orders_data(orders_df)
db_connector.upload_to_db(cleaned_orders_df, 'orders_table', db_connector.init_local_db_engine())

# %%---------------------------------------------------------
# M2task8: Retrieve and clean the date events JSON file from s3
s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
date_events_df = data_extractor.extract_from_s3_zone(s3_address)

cleaned_date_events_df = data_cleaner.clean_events_date(date_events_df)
db_connector.upload_to_db(cleaned_date_events_df, 'dim_date_table', db_connector.init_local_db_engine())
# %%
