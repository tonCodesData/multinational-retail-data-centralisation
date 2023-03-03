import yaml
import pandas as pd
from sqlalchemy import create_engine
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
    def read_rds_table(self, table_name, db_conn):
        engine = db_conn.init_db_engine()
        users = pd.read_sql_query(f"select * from {table_name}", engine)
        return users

