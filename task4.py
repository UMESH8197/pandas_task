from timeit import timeit

import pandas as pd
import mysql.connector as conn
from sqlalchemy import engine
import math
# 1 . load this data in sql and in pandas with a relation in sql

superstore_usa = pd.read_excel(r"C:\Users\user\Superstore_USA.xlsx")
# print(superstore_usa)
# print(superstore_usa.dtypes)

# superstore_usa = pd.read_excel(r"C:\Users\user\Superstore_USA.xlsx", sheet_name=None)
# print(superstore_usa)
# print(superstore_usa.keys())
# Creating DataFrame from dictionary
# df_orders = superstore_usa ['Orders']
# df_returns = superstore_usa['Returns']
# df_users = superstore_usa['Users']
# print(df_orders)
# print(df_returns)
# print( df_users)
df_returns = pd.read_excel(r"C:\Users\user\Superstore_USA.xlsx", sheet_name='Returns')
# print(df_returns)

df_user = pd.read_excel(r"C:\Users\user\Superstore_USA.xlsx", sheet_name='Users')
# print(df_user)

# mysql_settings = {"host":"localhost","user":"root","passwd":"root","allow_local_infile":True}
# mysql = conn.Connect(**mysql_settings)
# cursor = mysql.cursor()
# cursor.execute('CREATE DATABASE IF NOT EXISTS pandas_task')
# mysql_engin = engine.create_engine("mysql+pymysql://root:root@localhost/pandas_task")
# superstore_usa.to_sql('SuperStore_Usa',con=mysql_engin)

# 2 . while loading this data you dont have to create a table manually you can use any automated approach to create a table and load a data in bulk in table
# ANS. above answer stored data in bulk

# 3 . Find out how return that we have received and with a product id
df_prd_ret = superstore_usa.set_index('Order ID').join(df_returns,on='Order ID')[['Product Name']].drop_duplicates()
# print(df_prd_ret)

# 4 . try  to join order and return data both in sql and pandas
superstore_usa.set_index('Order ID').join(df_returns.set_index('Order ID'),on='Order ID')
# print(superstore_usa)

# 5 . Try to find out how many unique customer that we have
# print(superstore_usa['Customer Name'].drop_duplicates().count())
# 6 . try to find out in how many regions we are selling a product and who is a manager for a respective region
df_mgr = superstore_usa.join(df_user.set_index('Region'),on='Region',lsuffix='_left',rsuffix='_right')
# df_mgr = superstore_usa.join(df_user.set_index('Region'),on='Region',lsuffix='_left', rsuffix='_right')
# print(df_mgr.groupby('Manager')['Region'].count())
#df_mgr.keys()
#df_mgr
#df_mgr.dtypes


# 7 . find out how many different differnet shipement mode that we have and what is a percentage usablity of all the shipment mode with respect to dataset
df_shp = pd.DataFrame(superstore_usa['Ship Mode'].value_counts().reset_index().values,columns=['Ship Mode','Shp_Cnt'])
df_shp['pct']=df_shp['Shp_Cnt']/df_shp['Shp_Cnt'].sum()
# print(df_shp)

# 8 . Create a new column and try to find out a difference between order date and shipment date
superstore_usa['delta'] = (superstore_usa['Ship Date']-superstore_usa['Order Date']).dt.days
# print(superstore_usa[['Ship Date', 'Order Date', 'delta']])

# 9 . base on question number 8 find out for which order id we have shipment duration more than 10 days

# print(superstore_usa[superstore_usa.delta>10]['Order ID'])

# 10 . Try to find out a list of a returned order which shipment duration was more then 15 days and find out that region manager as well

# print(df_mgr.keys())

df_mgr = superstore_usa.join(df_user.set_index('Region'),on='Region')
df_del = df_mgr.join(df_returns.set_index('Order ID'),on='Order ID')
# print(df_del[df_del.delta > 15][['Order ID','Manager']])

# 11 . Group by region and find out which region is more profitable

df_prf = superstore_usa.groupby('Region')[['Profit']].max()
# print(df_prf)
# print(df_prf.nlargest(1,'Profit'))

# 12 . Try to find out overall in which country we are giving more discount
# Since country is USA always, I have calculated based on state
# print(superstore_usa.groupby('State or Province')['Discount'].sum())
# print(superstore_usa.groupby('State or Province')[['Discount']].sum().nlargest(1,'Discount'))

# 13 . Give me a list of unique postal code

# print(superstore_usa['Postal Code'].drop_duplicates())

# 14 . which customer segment is more profitable find it out .

# print(superstore_usa.groupby('Customer Segment')[['Profit']].sum().nlargest(1,'Profit'))

# 15 . try to find out the 10th most loss making product category .
# since we have only 3 product category, I have calculated based on product subcategories

df_prof_subcatg = superstore_usa.groupby('Product Sub-Category')[['Profit']].sum().sort_values(by='Profit')
df_prof_subcatg['Rnk'] = df_prof_subcatg['Profit'].rank()
# print(df_prof_subcatg)
# print(df_prof_subcatg[df_prof_subcatg.Rnk==10.0])

# 16 . Try to find out 10 top  product with highest margins
superstore_usa[['Product Name','Product Base Margin']].drop_duplicates().groupby('Product Name').filter(lambda x:x['Product Name'].count() > 1).sort_values(by='Product Name')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 500)
superstore_usa[superstore_usa['Product Name'] == 'Adesso Programmable 142-Key Keyboard']
# Based on above data , margin is based on Shipping Cost and Product Name
superstore_usa = superstore_usa[['Product Name','Shipping Cost','Product Base Margin']].drop_duplicates()
# print(superstore_usa.nlargest(10, 'Product Base Margin'))