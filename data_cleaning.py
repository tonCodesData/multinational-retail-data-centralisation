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
    
    #hod called clean_card_data in your DataCleaning class 
    # to clean the data to remove any erroneous values, 
    # NULL values or errors with formatting.
    def clean_card_data(self, df): 
        cleaned_df = df.dropna()
        # errors in dates, 
        # incorrectly typed values
        # rows filled with wrong information
        return cleaned_df
    
    def clean_store_data(self, df): 
        cleaned_df = df.dropna()
        # errors in dates, 
        # incorrectly typed values
        # rows filled with wrong information
        return cleaned_df
    
    def convert_product_weights(self, products_df):
        uniform_prod_weight_unit_df = products_df
        return uniform_prod_weight_unit_df

    def clean_products_data(self, uniform_prod_weight_unit_df): 
        cleaned_prod_df = uniform_prod_weight_unit_df.dropna()
        return cleaned_prod_df