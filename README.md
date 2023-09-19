# Multinational Retail Data Centralisation

In this project, I assume the role of a Data Analyst working for a multinational company that sells various goods across the globe. Their sales data was spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, it is imperative to make its sales data accessible from one centralised location. 

And so, my first goal was to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. Then I queried the database to get up-to-date metrics for the business.

## Tools and dependencies
- Python (pandas, tabula, requests, boto3, sqlalchemy)
- PostgreSQL (psql client, SQL Tools in VS Code)

## Data storage and File types

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

## Extract, Transform and Load(ETL):
In the etl_execution.py file, for each of the 6 information types(user, card, store, product, orders, and sales), the three different classes DataExtractor, DataCleaning, and DatabaseConnector are initialised. Then classses, as discussed previously, contain methods to create connection to source storage and extract data of each information type from different sources, clean the extracted data, and finally create connection to PostgreSQL server to load the cleaned data into sql database. 

## Create a star based schema for efficient data query:


