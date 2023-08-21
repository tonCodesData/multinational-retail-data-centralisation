import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

# this class is used to connect with and upload data to the database
class DatabaseConnector:
    def __init__(self) -> None:
        pass

    # read_db_creds() reads the credentials from yaml file and return a dictionary of the credentials
    def read_db_creds(self):
        with open('db_creds.yaml') as db_creds_file:
            db_creds = yaml.safe_load(db_creds_file)
        return db_creds
    
    # init_db_engine() reads the dictionary credentials from the return of read_db_creds(), and initialises and returns an sqlalchemy database engine.
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

    # list_db_tables() uses the engine from init_db_engine and lists all the tables in db
    def list_db_tables(self, engine):
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

    # upload_to_db() method takes in a Pandas DataFrame and table name to upload to database.
    def upload_to_db(self, df, table_name, engine):
        df.to_sql(name=table_name, con = engine, schema= 'public', if_exists='replace', index=False)
