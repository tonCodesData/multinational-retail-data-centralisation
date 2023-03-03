class DataCleaning:
    # clean data of each data sources
    def clean_user_data(self, df):
        # perform data cleaning operations
        cleaned_df = df.dropna() # example: drop rows with null values
        return cleaned_df
    