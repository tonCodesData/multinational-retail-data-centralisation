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
    
    #m2t7
    #step3: 
    def clean_orders_data(self, orders_df):
        # You should remove the columns, first_name, last_name 
        # and 1 to have the table in the correct form before uploading to the database.
        # You will see that the orders data contains column headers 
        # which are the same in other tables.
        # This table will act as the source of truth for your sales data 
        # and will be at the center of your star based database schema.
        cleaned_orders_df = orders_df
        return cleaned_orders_df

    #m2t8 
    def clean_events_date(self, date_events_df):
        cleaned_date_events_df = date_events_df
        return cleaned_date_events_df