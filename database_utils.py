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