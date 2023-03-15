from sqlalchemy import create_engine
import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
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

db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaner = DataCleaning()

db_creds = db_connector.read_db_creds()
engine = db_connector.init_db_engine(db_creds)

table_name_ls = db_connector.list_db_tables(engine)
for i in range (len(table_name_ls)): 
    print (table_name_ls[i])

# Use the read_rds_table method 
# to extract the table containing user data 
# and return a pandas DataFrame.
users = data_extractor.read_rds_table('legacy_users', engine)
users_cleaned = data_cleaner.clean_user_data(users)

db_connector.upload_to_db(users_cleaned, 'dim_users', db_connector.init_local_db_engine())