import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
class DatabaseConnector:
    # will be used to connect with and upload data to the database
    # task 3 of m2
    def read_db_creds(self, creds_file='db_creds.yaml'):
        # read credentials from yaml file and returns a dictionary 
        with open(creds_file, 'r') as file: 
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self): 
        # read credentials from read_db_creds, initialise and return an sqldatabase engine
        creds = self.read_db_creds()
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = 5432
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    # step 4: Add a list_db_tables method to the DatabaseConnector class 
    # to list all the tables in the database.
    def list_db_tables(self, creds_file): 
        engine = self.init_db_engine(creds_file)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
        '''
        engine = self.init_db_engine()
        with engine.connect() as conn:
            tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            table_names = [table[0] for table in tables]
        return table_names
        '''

    
    
    ''' step 7: Add an upload_to_db method to the DatabaseConnector class 
    to upload a Pandas DataFrame to a database table.'''
    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    










'''RDS_HOST: data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com
RDS_PASSWORD: AiCore2022
RDS_USER: aicore_admin
RDS_DATABASE: postgres
RDS_PORT: 5432'''

        print(DatabaseConnector.read_db_creds())

DatabaseConnector.init_db_engine()

