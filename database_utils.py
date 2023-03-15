import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DatabaseConnector:
    # will be used to connect with and upload data to the database
    def __init__(self) -> None:
        pass

    # step2: Create a method read_db_creds 
    # this will read the credentials yaml file 
    # and return a dictionary of the credentials.
    def read_db_creds(self):
        with open('db_creds.yaml') as db_creds_file:
            db_creds = yaml.safe_load(db_creds_file)
        return db_creds
    
    # step3: Now create a method init_db_engine 
    # which will read the credentials 
    # from the return of read_db_creds 
    # and initialise and return an sqlalchemy database engine.
    def init_db_engine(self, db_creds):
        HOST = db_creds['RDS_HOST']
        PORT = db_creds['RDS_PORT']
        USER = db_creds['RDS_USER']
        PASSWORD = db_creds['RDS_PASSWORD']
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        DATABASE = db_creds['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    #step 4: Using the engine from init_db_engine 
    # create a method list_db_tables to list all the tables in db
    # so you know which tables you can extract data from. 
    # Develop a method inside your DataExtractor class 
    # to read the data from the RDS database.

    def list_db_tables(self, engine):
        # list all the tables
        ls_db_table_names = inspect(engine).get_table_names()
        return ls_db_table_names
    
    def init_local_db_engine(self):
        HOST = 'localhost'
        PORT = 5432
        USER = 'postgres'
        PASSWORD = '12345'
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        DATABASE = 'Sales_Data'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    #step 7: Now create a method in your DatabaseConnector 
    # class called upload_to_db.
    # This method will take in a Pandas DataFrame and table name 
    # to upload to as an argument.
    def upload_to_db(self, df, table_name, engine):
        df.to_sql(name=table_name, con = engine, schema= 'public', if_exists='replace', index=False)