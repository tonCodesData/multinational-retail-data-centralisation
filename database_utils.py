import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
class DatabaseConnector:
    # will be used to connect with and upload data to the database
    def __init__(self) -> None:
        pass
    # step2: Create a method read_db_creds 
    # this will read the credentials yaml file 
    # and return a dictionary of the credentials.
    def read_db_creds(self, creds_filename):
        file = open(creds_filename, 'r')
        creds = yaml.safe_load(file)
        return creds
    
    # step3: Now create a method init_db_engine 
    # which will read the credentials 
    # from the return of read_db_creds 
    # and initialise and return an sqlalchemy database engine.
    def init_db_engine(self):
        creds = self.read_db_creds()    
        HOST = creds['RDS_HOST']
        PORT = creds['RDS_PORT']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        DATABASE = creds['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    #step 4: Using the engine from init_db_engine 
    # create a method list_db_tables to list all the tables in db
    # so you know which tables you can extract data from. 
    # Develop a method inside your DataExtractor class 
    # to read the data from the RDS database.
    def list_db_tables(self): 
        engine  = self.init_db_engine()
        # list all the tables
        table_name_ls = inspect(engine).get_table_names()
        return table_name_ls
    



