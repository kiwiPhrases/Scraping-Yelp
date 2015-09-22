import os, re
import pandas as pd

"""
###################################################################################################
INPUT:
    1. SanDiego_biz_collection.csv -- the collection of all businesses collected
        ...through Yelp's API.
OUTPUT:
    2. [category]_SanDiego_biz.csv -- the collection of businesses that fit a category of interest. 
###################################################################################################
DESCRIPTION: Unfortunately, Yelp does not provide convenient 'restaurant' labels within 
its API, although it does in the API query but that would've been a more targeted look. Consequently,
we need to look at the categories of collected businesses and using key words tease out the 
category we desire. 

NOTE:
I have already looked through and collected the words for restaurants and shops.    
###################################################################################################
"""
############################# 0. Setup #############################################

data_path="c:/users/gene/documents/duke/dropbox/gene/yelp_scrapping"
os.chdir(data_path)

api_data=pd.read_csv('SanDiego_biz_collection.csv', encoding='latin-1')

############################# I. Get pertinent data ################################
api_data=api_data[~(api_data.categories.isnull())]

food_list=['restaurant','grill','bar','cafe','bakery','pub','bbq','sushi','coffee',
'food','eat','food truck','juice','drinks', 'tea', 'deli', 'seafood', 'cajun', 'icecream',
'brunch', 'lunch', 'dinner', 'breakfast', 'bars', 'sandwiches', 'fast food', 'pizza', 'sandwiches',
'buffet', 'mediterrenean', 'bakeries', 'steak', 'divebar', 'mexican','italian', 'hawaiian', 'french', 
'dessert', 'yogurt', 'korean', 'bagels','asian fusion', 'chinese', 'soup', 'vegan', 'vegetarian', 'burgers']
food_str="|".join(food_list)

shop_list=['shop', 'shopping', 'candy store', 'candy', 'store','beer, wine & spirits', 'wine' ]
shop_str="|".join(shop_list)

food_data=api_data[api_data.categories.str.contains(food_str)]
shopping_data=api_data[api_data.categories.str.contains(shop_str)]

food_data.to_csv("restaurants_SanDiego_biz.csv", index=False)
shopping_data.to_csv('shopping_SanDiego_biz.csv', index=False) 