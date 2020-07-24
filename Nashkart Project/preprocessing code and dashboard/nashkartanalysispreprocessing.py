# IMPORTING PACKAGES
import pymysql
import pandas as pd
import numpy as np

# CONNECTING TO THE DATABASE

host="*******"
port=****
dbname="****"
user="****"
password="******"

conn = pymysql.connect(host, user=user,port=port,
                           passwd=password, db=dbname)

# WORKING WITH THE TABLES
details_df=pd.read_sql('select * from  Nashkart_details;', con=conn) 

items_df=pd.read_sql('select * from  Nashkart_orderitem;', con=conn)

product_df=pd.read_sql('select * from  Nashkart_product;', con=conn)


# 1. WORKING WITH USER DETAILS

# KNOWING ABOUT DATA

print(details_df)
print(details_df.columns)
print(details_df.info)
print(details_df.describe())
print(details_df.dtypes)
print(details_df)


# CLEANING THE DATA

print(details_df.isnull().sum())
print(pd.isnull(details_df))
print(details_df.isnull().sum())

details_df= details_df.drop('user_id', axis=1)
'''
print(items_df.isnull().sum())

print(items_df[items_df['product_id'].isnull()])

items_df=items_df.dropna()


print(items_df.isnull().sum())

items_df= items_df.drop(['id','date_added'], axis=1)

print(items_df.head())
'''

# SPLITTING THE DATE AND TIME FOR THE CONVINENCES

details_df['sale hour'] = details_df['date_added'].dt.hour
details_df['sale year'] = details_df['date_added'].dt.year
details_df['sale month'] = details_df['date_added'].dt.month
details_df['sale day'] = details_df['date_added'].dt.day

print(details_df.head())

# CLEANING THE COUNTRY

details_df.rename(columns={'zipcode':'country'},inplace=True)
print(details_df.head())
print(details_df['country'].unique())
print(details_df.loc[details_df['country']=="Heaven"])

details_df.drop([107,108,112],inplace=True)

print(details_df['country'].unique())

for i in details_df['country'].unique():
    if i == 'USA':
         details_df['country'].mask(details_df['country']== i , "United States of America" , inplace = True)
    else:
        details_df['country'].mask(details_df['country']== i , "India" , inplace = True)

print(details_df['country'].unique())

# CLEANING THE STATE
        
print(details_df['state'].unique())

details_df.loc[(details_df.state == 'tamil nadu'),'state']='Tamil Nadu'
details_df.loc[(details_df.state == 'Tamil nadu'),'state']='Tamil Nadu'
details_df.loc[(details_df.state == 'tn'),'state']='Tamil Nadu'
details_df.loc[(details_df.state == 'TAMIL NADU'),'state']='Tamil Nadu'
details_df.loc[(details_df.state == 'Tamilnadu '),'state']='Tamil Nadu'
details_df.loc[(details_df.state == 'Tamilnadu'),'state']='Tamil Nadu'
details_df.loc[(details_df.state == 'Tamil Nadu'),'state']='Tamil Nadu'
details_df.loc[(details_df.state == 'Tamil  Nadu'),'state']='Tamil Nadu'
details_df.loc[(details_df.state == ''),'state']='Tamil Nadu'
details_df.loc[(details_df.state == 'Delhi'),'state']='New Delhi'
details_df.loc[(details_df.state == 'new delhi'),'state']='New Delhi'
details_df.loc[(details_df.state == 'Madhya Pradesh '),'state']='Madhya Pradesh'
details_df.loc[(details_df.state == 'madhya pradesh'),'state']='Madhya Pradesh'
details_df.loc[(details_df.state == 'maharastra'),'state']='Maharastra'
details_df.loc[(details_df.state == 'Maharashtra'),'state']='Maharastra'
details_df.loc[(details_df.state == 'west bengal'),'state']='West Bengal'

print(details_df['state'].unique())

# CLEANING THE CITY

print(details_df['city'].nunique())
print(details_df['city'].unique())

details_df.loc[(details_df.city == 'chennai'),'city']='Chennai'
details_df.loc[(details_df.city == 'Chennai'),'city']='Chennai'
details_df.loc[(details_df.city == 'CHENNAI'),'city']='Chennai'
details_df.loc[(details_df.city == 'Chennai '),'city']='Chennai'
details_df.loc[(details_df.city == 'kilpauk'),'city']='Chennai'
details_df.loc[(details_df.city == 'madurai'),'city']='Madurai'
details_df.loc[(details_df.city == 'salem'),'city']='Salem'
details_df.loc[(details_df.city == 'bangalore'),'city']='Bangalore'
details_df.loc[(details_df.city == 'Banglore'),'city']='Bangalore'
details_df.loc[(details_df.city == 'Bangolre'),'city']='Bangalore'
details_df.loc[(details_df.city == 'banglore'),'city']='Bangalore'
details_df.loc[(details_df.city == 'mumbai'),'city']='Mumbai'
details_df.loc[(details_df.city == 'pune'),'city']='Pune'
details_df.loc[(details_df.city == ' new delhi'),'city']='New Delhi'
details_df.loc[(details_df.city == 'Delhi'),'city']='New Delhi'
details_df.loc[(details_df.city == 'delhi'),'city']='New Delhi'
details_df.loc[(details_df.city == 'thiruchi'),'city']='Thiruchi'
details_df.loc[(details_df.city == 'kokata'),'city']='Kolkata'

print(details_df['city'].unique())
print(details_df['city'].nunique())

print(details_df)


# 2. WORKING WITH PRODUCT DATA

print(product_df.info())

print(product_df)

product_df=product_df.drop(['digital','image'],axis=1)

print(product_df)



# 2. WORKING WITH ITEMS ORDERED DATA

print(items_df.info())

print(items_df.head())

items_df=items_df.drop(['date_added'],axis=1)

print(items_df.head())


# CREATING CATEGORY

items_df['Category'] = items_df.product_id.map(lambda x: 'Books' if x in range(1,11) else 'Electronics'  if x in range(11,19) else 'Fruits'  if x in range(19,24) else 'Vegetables'  if x in range(24,29) else 'Provision'  if x in range(29,32) else 'Clothing')

# REPLACING PRODUCT ID WITH PRODUCT NAME

product_df_2=product_df.copy()

print(product_df_2.head())

product_df_2=product_df_2.drop('price',axis=1)

print(product_df_2.head())

dict_df=product_df_2.set_index('id').T.to_dict('list')

for key in dict_df:
     print(key, '->', dict_df[key])
     
trial_items=items_df.copy()

print(trial_items.head())

for key in dict_df:
    for i in trial_items['product_id']:
        if key == i:
            trial_items.loc[(trial_items.product_id == i ),'product_id']= dict_df[key]     

print(trial_items.head())

print(trial_items.tail())

trial_items.rename(columns = {'product_id':'Product'}, inplace = True)

print(trial_items.head())

# CONVERTING DATAFRAME TO CSV 

trial_items.to_csv(r'C:\Users\Karthick Raja\Desktop\nashkart order_table.csv', index = False)

items_df.to_csv(r'C:\Users\Karthick Raja\Desktop\nashkart items.csv', index = False)

product_df.to_csv(r'C:\Users\Karthick Raja\Desktop\nashkart products.csv', index = False)
