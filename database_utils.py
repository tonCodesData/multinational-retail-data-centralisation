import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
class DatabaseConnector:
    # will be used to connect with and upload data to the database

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
