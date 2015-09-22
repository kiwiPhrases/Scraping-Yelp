import os, re
import pandas as pd
import numpy as np
import time
import rauth
import pickle as pkl

"""
##############################################################################################
INPUT:
    1. rescan_nodes.csv --a list of coordinates that cover San Diego or subsections thereof. 
    2. sparse/medium/dense_rescan_nodes.csv -- a list similar to the above at finer resolution.
OUTPUT:
    1. SanDiego_biz.csv -- list of businesses in the San Diego area
##############################################################################################
DESCRIPTION:
    Use Yelp's API to gather a list of all businesses in San Diego by scanning the area
    repeatedly at ever-finer resolution in selected areas. 
    
API: https://www.yelp.com/developers/documentation/v2/search_api

Radius and degrees info: https://en.wikipedia.org/wiki/Decimal_degrees    
###############################################################################################
"""

#################################### 0. SETUP ##################################################
"""
data_path='c:/users/gene/documents/duke/dropbox/gene/yelp_scrapping'
os.chdir(data_path)
coords=pd.read_csv('SDsearch_nodes.csv')

auth_dict={
'consumer_key':"84EtbY0wnqaOrP2Rb1nIVQ",
'consumer_secret':"uOoyU3BzvLi5MJ2JMlS_IOImcuo",
'token':"1GZ2219irMBWOc1nvRi2PmO7rsyypP8U",
'token_secret':"YJVMg-FmOdXQqIyLgyUaZH5i2yw"
}
"""

################################################################################################
##################################### 1. FUNCTIONS #############################################    

##################################### 0. Data Fecthing Functions ###############################
def get_search_parameters(lat,lon,r=300):
  """Code Credit: https://www.yelp.com/developers/documentation/v2/search_api"""
  params = {}
  #params['bounds']="{},{}|{},{}".format(str(sw_lat),str(sw_long),str(ne_lat), str(ne_long)) #sw_lat, sw_long, ne_lat, ne_long
  #params["term"] = "restaurant"
  params["ll"] = "{},{}".format(str(lat),str(lon))
  params["radius_filter"] = r
  #params["limit"] = "10"    
  return(params)  
  
def get_results(params):
    """Code Credit: https://www.yelp.com/developers/documentation/v2/search_api"""
    consumer_key = "84EtbY0wnqaOrP2Rb1nIVQ"
    consumer_secret = "uOoyU3BzvLi5MJ2JMlS_IOImcuo"
    token = "1GZ2219irMBWOc1nvRi2PmO7rsyypP8U"
    token_secret = "YJVMg-FmOdXQqIyLgyUaZH5i2yw"
   
    session = rauth.OAuth1Session(
    consumer_key = consumer_key
    ,consumer_secret = consumer_secret
    ,access_token = token
    ,access_token_secret = token_secret)
     
    request = session.get("http://api.yelp.com/v2/search",params=params)
   
    #Transforms the JSON API response into a Python dictionary
    if request.status_code==200:
        data = request.json()
        session.close()
        return(data)
    else:
        print("Status not OK")
        return("error")

################################ I. Data Processing Functions #################################     
def get_fields(entry):
    data_dict={}
    extract=[
    'rating',
    'is_claimed',
    'name',
    'review_count',
    'url',
    'categories',
    'phone',
    'is_closed',
    'image_url',
    'id']
    
    for key in extract:
        try:
            data_dict[key]=entry[key]
        except KeyError:
            data_dict[key]=np.nan
            
    loc_content=entry['location'].keys()
    for key in loc_content:
        try:
            data_dict[key]=entry['location'][key]
        except KeyError:
            data_dict[key]=np.nan
    return(data_dict)
    
def create_container():
    fields=[
    'rating',
    'is_claimed',
    'name',
    'review_count',
    'display_address',
    'url',
    'city',
    'is_closed',
    'phone',
    'image_url',
    'coordinate',
    'categories',
    'id']
    data_dict={}
    for field in fields:
        data_dict[field]=[]
    return(data_dict)
 
def enter_record(entry_dict, data_dict):
    for key in data_dict.keys():
        try:
            data_dict[key].append(entry_dict[key])
        except KeyError:
            data_dict[key].append(np.nan)
            
def fix_records(data_dict):
    data_dict['display_address']=[" ".join(item) for item in data_dict['display_address']] #needs to be united
    data_dict['categories']=[str(item ).lower() for item in data_dict['categories']] #needs to be flattened
    data_dict['categories']=[re.sub("\[|\]|\'","",item) for item in data_dict['categories']] #further cat processing
    coordinate_list=[]
    for item in data_dict['coordinate']:
        #print(item)
        if item is np.nan:
            coordinate_list.append(item)
        if item is not np.nan:
            coordinate_list.append('{},{}'.format(str(item['latitude']), str(item['longitude'])))
    data_dict['coordinate']=coordinate_list
        
def process_dict(data_dict, csv_name='api_biz_data.csv'):
    len_list=[len(data_dict[key]) for key in data_dict.keys()]
    #print("--"*20)
    if all([item==len_list[0] for item in len_list]):
        #print("Making data frame using the data dictionary")
        df=pd.DataFrame(data_dict).drop_duplicates()
        df.to_csv(csv_name, index=False)
        print("\nData saved to: %s" %csv_name)
    else:
        print("Data length in keys are not of the same length")
        print(data_dict.keys())
        print(len_list)
        df=None
    return(df)
    
def get_sqr_total(resp_data, block):
    print("--"*20)
    try:
        print("Total number of businesses in this sqrt: %d" %resp_data['total'])
        if resp_data['total']>40:
            print("\n"+"!"*5+" Radius is too large to capture all businesses in this search block"+"!"*5)
            large_count(block,resp_data['total'])
    except KeyError:
        print("Total number of businesses not found")
    return(resp_data['total'])
    print("--"*20)

def get_square(businesses,block=0):
    data_dict=create_container()
    
    for business in businesses:
        entry_dict=get_fields(business)
        enter_record(entry_dict, data_dict)  
    fix_records(data_dict)
    data_dict['block']=[block]*len(data_dict['name'])
    df=process_dict(data_dict)
    return(df)
    
def SDbiz(file_name="SanDiego_biz.csv"):
    data_dict=create_container()
    data_dict['block']=[]
    pd.DataFrame(data_dict).to_csv(file_name, index=False)
    
def append_df(df, biz_df="SanDiego_biz_sparse_medium.csv"):
    try:
        update_df=pd.read_csv(biz_df)
    except UnicodeDecodeError:
        update_df=pd.read_csv(biz_df, encoding='latin-1')
    try:
        updated_df=pd.concat([update_df.sort(axis=1), df.sort(axis=1)], axis=0)
        updated_df.to_csv(biz_df, index=False)
        print("--"*20)
        print("%s updated" %biz_df)
        print("=="*30)
        return(updated_df)
    except:
        print("Couldn't create the DataFrame, there must be an error of some type")
    
############################ II. Convenience Functions ################################    
def Pickle(data,file_name='businesses_block.pkl'):
        with open(file_name, 'wb') as f:
            pkl.dump(data, f)
        print("Downloaded JSON data pickled to [%s]" %file_name)
        
def eat_pickle(file_name='businesses_block.pkl'):
    with open(file_name, 'rb') as f:
        return(pkl.load(f))        

def write_count(count):
    with open('Count_GENE.txt', 'w') as the_file:
        the_file.write(str(count))
 
def read_count(file_name='Count_GENE.txt'):
    """
    OUTPUT:
        1. count -- read in count.txt to know where to continue from previous stop point
    """
    with open(file_name) as f:
        count=f.readline()
        return(int(count))    

def large_count(block, count, file_name='blocks_to_rescan_sparse_medium.txt'): #LINE CHANGED
    with open(file_name, 'a') as f:
        f.write("block:{}; count:{}".format(str(block), str(count)))
        
def counter_reset():
    answer='the cake is a lie!'
    while (answer!='Y') & (answer!='N'):
        answer=input("Would you like to reset the counter to 0? [Y/N]: ")
    if answer == "Y":    
        write_count(0)
        print("Counter reset")
    
#######################################################################################        
########################### III. MAIN Function ########################################        
def main():
    data_path='c:/users/gene/documents/duke/dropbox/gene/yelp_scrapping'
    os.chdir(data_path)
    
    counter_reset()
    #coords=pd.read_csv('SDsearch_nodes.csv')
    coords=pd.read_csv('rescan_nodes.csv')
    
    count=read_count()
    df=coords[count:]
    
    for (lon,lat,i) in zip(df.X.tolist(), df.Y.tolist(),df.index.tolist()):
        print("On block: %d" %i)
        params=get_search_parameters(lat, lon, r=7)
        resp_data=get_results(params)
        
        print("=="*30)
        if 'error' in resp_data.keys():
            print('\nERROR:')
            if resp_data['error']['id']=='EXCEEDED_REQS':
                print(resp_data['error']['text'])
                print("Exiting loop.")
                break
            else:
                print("Will try the next block..")
                pass
            
        if 'error' not in resp_data.keys():
            Pickle(resp_data) #quick and dirty backup
            total=get_sqr_total(resp_data, block=i)
            if total==0:
                #print("No businesses in this region.")
                pass
            if total>0:
                businesses=resp_data['businesses']
                block_df=get_square(businesses, block=i)
                append_df(block_df)
        write_count(count=i)
        
if __name__=="__main__":
        main()