# def clean_orders_data(self, orders_df):
#     # You should remove the columns, first_name, last_name 
#     # and 1 to have the table in the correct form before uploading to the database.
#     # You will see that the orders data contains column headers 
#     # which are the same in other tables.
#     # This table will act as the source of truth for your sales data 
#     # and will be at the center of your star based database schema.
#     cleaned_orders_df = orders_df
#     return cleaned_orders_df


#%%
import pandas as pd
#%%
df = pd.read_csv('orders.csv')
#%%
pd.set_option('display.max_rows', 1000) 
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
#%%
df.drop(['Unnamed: 0', 'level_0', 'first_name', 'last_name', '1'], axis=1, inplace=True)
# %%
df['product_code'] = df['product_code'].str.upper()
# %%
# %%
