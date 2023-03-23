#%%
import pandas as pd
import tabula as tb
import requests as req
import boto3
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

# %%
data_extractor = DataExtractor()
# %%
card_df = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

#%%
type(card_df)
# %%
card_df
# %%
card_df.to_csv('card.csv')
# %%
