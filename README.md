# Multinational Retail Data Centralisation

In this project, I assume the role of a Data Analyst working for a multinational company that sells various goods across the globe. Their sales data was spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, it is imperative to make its sales data accessible from one centralised location. 

And so, my first goal was to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. Then I queried the database to get up-to-date metrics for the business.

## Tools and dependencies
- Python (pandas, tabula, requests, boto3, sqlalchemy)
- PostgreSQL (psql client, SQL Tools in VS Code)

## data storage and file types

|serial| data regarding      | stored in     | file type |
|:-----| :-------------------| :------------ |:----------|
|1     | historical user data| AWS RDS       |           |
|2     | users card details  | AWS S3 bucket | PDF       |
|3     | store data          | API           |           |
|4     | product information | AWS S3 bucket | CSV       |
|5     | orders              | AWS RDS       |           |
|6     | sales               | AWS S3 bucket | JSON      | 

## File description

|File | Description |
|-----|-------------|
|data_extraction.py| contain DataExtractor class. This works as a utility class by creating methods to help extract data from different data sources |
|data_cleaning.py| contain DataCleaning class to clean data of each data sources |
|database_utils.py| contain DatabaseConnector class. This class is used to connect with and upload data to the database |


## Extract and clean data from data sources:

##### data:
- historial data of users --> AWS RDS database in cloud
    -- DatabaseConnector
        - db_creds.yaml
        - read_db_creds(db_creds.yaml) --> return dict of creds
        - init_db_engine(read_db_creds(db_creds.yaml)) --> return sqlalchemy db engine
        - list_db_tables(init_db_engine(read_db_creds(db_creds.yaml))) --> list all tables in db
    -- DataExtractor
        DatabaseConnector.list_db_tables --> get user table name
        - read_rds_table(user table name) --> pd df
    -- DataCleaning
        - clean_user_data(DataExtractor.read_rds_table) --> pd df
    -- DatabaseConnector
        - upload_to_db(DataCleaning.clean_user_data) --> store data in sales_data as table dim_users

- user's card details --> PDF doc in AWS S3 bucket
    ** tabula-py needs to be installed
    -- DataExtractor
        - retrieve_pdf_data(pdf link) --> pd df
    -- DataCleaning
        - clean_card_data(DataExtractor.retrieve_pdf_data) --> pd df
    -- DatabaseConnector
        - upload_to_db(DataCleaning.clean_card_data) --> store data in sales_data as table dim_card_details

- each store's data --> using API
    This API has two GET-methods(endpoints):  
        1. num_of_store_endpoint: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores
        --> return numer of stores
        2. a_store_endpoint: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number} 
        --> retrieve a store

    There is a x-api-key with the value yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX. This gets saved in a dictionary. This is included in the list_number_of_stores method to connect to the API.  

    -- DataExtractor
        - list_number_of_stores(num_of_store_endpoint, x-api-key) --> returns number of stores to extract
        - retrieve_stores_data(a_store_endpoint) --> retrieve all stores from API --> save as pd df
    -- DataCleaning
        - clean_store_data(DataExtractor.retrieve_stores_data) --> pd df
    -- DatabaseConnector
        - upload_to_db(DataCleaning.clean_card_data) --> store data in sales_data as table dim_store_details

- information for each product of company --> CSV format in AWS S3 bucket
    ** download and extract info --> using boto3 package --> return a pd df
    S3 address:- s3://data-handling-public/products.csv
    -- DataExtractor
        - extract_from_s3(S3 address) --> return pandas DataFrame.
        note- login to AWS CLI before retrieving
    -- DataCleaning
        - convert_product_weights
        - clean_products_data
    -- DatabaseConnector
        - upload_to_db(DataCleaning.clean_products_data) --> store data in sales_data as table dim_products

- orders table --> AWS RDS
    -- DataExtractor
        DatabaseConnector.list_db_tables --> get orders table name
        - read_rds_table(orders table name) --> pd df
    -- DataCleaning
        - clean_orders_data(DataExtractor.read_rds_table) --> pd df
    -- DatabaseConnector
        - upload_to_db(DataCleaning.clean_orders_data) --> store data in sales_data as table orders_table

- sale date --> JSON file in S3
    ** download and extract info --> using boto3 package --> return a pd df
    S3 address:- https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json
    
    -- DataExtractor
        - extract_from_s3_zone(self, s3_address) --> return pandas DataFrame
        note- login to AWS CLI before retrieving
    -- DataCleaning
        - clean_events_date
    -- DatabaseConnector
        - upload_to_db(DataCleaning.clean_date_data) --> store data in sales_data as table dim_date_times

----------------------------------------------------------------------------------------------------------


