class DataCleaning:
    # clean data of each data sources
    def __init__(self) -> None:
        pass
    def clean_user_data(self, df): 
        # clean user data
        # look for 
        # NULL values, 
        cleaned_df = df.dropna()
        # errors in dates, 
        # incorrectly typed values
        # rows filled with wrong information
        return cleaned_df