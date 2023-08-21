from sqlalchemy import create_engine
import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import tabula as tb
import requests as req
import boto3

# This class works as a utility class by creating methods to help extract data from different data sources.
class DataExtractor:
    def __init__(self) -> None:
        pass

    # user data is stored in aws rds. the following method extract data from there
    # read_rds_table() extracts the database table to a pandas DataFrame.
    # It takes in an instance of DatabaseConnector class and the table name as an argument and return a pandas Df
    def read_rds_table(self, table_name, engine):
        users = pd.read_sql_query(f"select * from {table_name}", engine)
        return users
    
    # retrieve_pdf_data() method extracts all pages from a pdf using tabular-py python package, and returns df.
    def retrieve_pdf_data(self, link):
        ls = tb.read_pdf(link, multiple_tables=True, pages = 'all')
        df = pd.concat(ls)
        return df

    # The store data can be retrieved through the use of an API with two GET methods. 
    # One returns the number of stores in the business and the other retrieves a store given a store number.
    # Create a dictionary to store the header details it will have a key x-api-key with the value yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX.
    # The two endpoints for the API are as follows:
    # Retrieve a store: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}
    # Return the number of stores: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores

    # returns number of stores to extract
    def list_number_of_stores(self, num_stores_endpoint, header_dict):
        response = req.get(num_stores_endpoint, headers = header_dict)
        json_data = response.json()
        return json_data['number_stores']        

    # extracts all stores and save them in pandas df
    # def retrieve_stores_data(self, num_stores_endpoint, header_dict):
    #     response = req.get(num_stores_endpoint, headers = header_dict)
    #     data = response.json()
    #     return pd.DataFrame(data)

    # retrieve_stores_data() method takes the endpoint as argument and extracts all stores from the API saving as pandas DataFrame
    def retrieve_stores_data(self, retrieve_store_endpoint, number_of_stores, header_dict):
        stores_ls = []
        for store_number in range(number_of_stores):
            response = req.get(retrieve_store_endpoint.format(store_number = store_number), headers = header_dict)
            data = response.json()
            stores_ls.append(data)
        stores_df = pd.DataFrame(stores_ls)
        return stores_df

    # The information for each product the company currently sells is stored in CSV format in an S3 bucket on AWS.
    def extract_from_s3_zone(self, s3_address): 
        s3 = boto3.client('s3')
        bucket_name = s3_address.split('/')[2].split('.')[0]
        file_path_n_name = '/'.join(s3_address.split('/')[3:])
        with open('date_needs_cleaning.json', 'wb') as date_json:
            s3.download_fileobj(bucket_name, file_path_n_name, date_json)
        date_data = pd.read_json('date_needs_cleaning.json')
        date_df = pd.DataFrame(date_data)
        return date_df
    
    def extract_from_s3(self, s3_address): 
        s3 = boto3.client('s3')
        bucket_name = s3_address.split('/')[2]
        file_path_n_name = '/'.join(s3_address.split('/')[3:])
        with open('products.csv', 'wb') as prod_csv:
            s3.download_fileobj(bucket_name, file_path_n_name, prod_csv)
        products_data = pd.read_csv('products.csv')
        products_df = pd.DataFrame(products_data)
        return products_df
    
    
