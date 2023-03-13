import yaml
import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector as dc
class DataExtractor():
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
    def read_rds_table(self, table_name, db_conn):
        engine = db_conn.init_db_engine()
        users = pd.read_sql_query(f"select * from {table_name}", engine)
        return users

    # Use your list_db_tables method 
    # to get the name of the table containing user data.
    table_name_ls = dc.list_db_tables('db_creds.yaml')
    for i in range (len(table_name_ls)): 
        print (table_name_ls[i])

    # Use the read_rds_table method 
    # to extract the table containing user data 
    # and return a pandas DataFrame.
    users = read_rds_table(filename='db_creds.yaml', table_name='legacy_users')



