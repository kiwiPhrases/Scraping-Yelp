import requests
import os, re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from numpy import random as rnd
import time
import pickle as pkl
import re
import json

"""
##############################################################################################################################
INPUT:
    1. biz_reviews_collection.json --collection of reviews
OUTPUT:
    1. pickled_user_data.pkl -- scraped data that's saved along the way
    2. SanDiego_users.csv -- if successful then scraped data is saved into this csv file.
    3. count.txt -- keeps track of the number of pages processed. 
##############################################################################################################################
DESCRIPTION:
    Read in user IDs from the reviews, construct URLS, and gather data on each user. 
##############################################################################################################################
"""


#################################### 0. SETUP ##################################################
data_path="c:/users/gene/documents//duke/dropbox/gene/yelp_scrapping"
os.chdir(data_path)

################################################################################################
##################################### 1. FUNCTIONS ############################################# 

############################### I. Scraping Functions ###############################
def fetch_website(url):
    """
    To hide that the scraping is being done via Python, I change the user-agent. The numerous user-agents
    included herein are simply the ones most commonly used at the time of scraping. Having multiple agents and
    having ones picked randomly did not help. Yelp still occasionally returned fake websites. 
    """
    user_agent_list=[
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0',
    'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36']
    user_agent=give_rndm_userAgent(user_agent_list)
    print(user_agent['User-agent'])
    r=requests.get(url, headers=user_agent)
    try:
        print("Accessed and downloaded URL data")
        return(r.content)
    except ConnectionError:
        print("Incurred the infamous connection error")
        print("Skipping this url")
        return("Skip")

def fetch_user_reviews(user_id, num_reviews,base_url="http://www.yelp.com/user_details_reviews_self?rec_pagestart=0&userid="):
    ##NOTE: some users will probably have hundreds or thousands of reviews. For now, we restrict to collecting at most 50 reviews
    soup=BeautifulSoup(fetch_website(base_url+user_id))
    Master_reviews_list=soup.findAll("div", class_='review')
        
    if (num_reviews>50):
        print("Seriously prolific user with [%d] reviews" %num_reviews)
    if (num_reviews>9) & (num_reviews<200):
        review_links=[x.attrs['href'] for x in soup.findAll('a', class_='page-option available-number')]
        rev_soup_list=[]
        for url in review_links:
            wait()
            rev_soup_list.append(BeautifulSoup(fetch_website(url)))
        list_reviewsList=[rev_soup.findAll("div", class_='review') for rev_soup in rev_soup_list]  
        for reviewsList in list_reviewsList:
            Master_reviews_list.extend(reviewsList)        
    print("# of reviews: %d\n# reviews got: %d" %(num_reviews, len(Master_reviews_list)))       
    return(Master_reviews_list)   
            
    
def fetch_user_friends(user_id, num_friends,base_url="http://www.yelp.com/user_details_friends?userid="):
    #j=0
    friend_set=[]
    friend_list=[]
    wait()
    soup=BeautifulSoup(fetch_website(base_url+user_id))
    master_user_info_list=soup.findAll("ul", class_="user-passport-info")
    master_user_stats_list=soup.findAll('ul', class_='user-passport-stats')
    if len(soup.findAll('a', class_='page-option available-number'))>0:
        friends_links=[x.attrs['href'] for x in soup.findAll('a', class_='page-option available-number')]
        friends_soup_list=[BeautifulSoup(fetch_website(url)) for url in friends_links]
        list_friendsList=[friend_soup.findAll("ul", class_="user-passport-info") for friend_soup in friends_soup_list] 
        for friendsList in list_friendsList:
            master_user_info_list.extend(friendsList)
    
    for user_info in master_user_info_list:
        id_link=user_info.find('a').attrs['href']
        friend_list.append(re.search('userid=(\S+)', id_link).group(1))
    friend_set=list(set(friend_list))            
    print('# of friends: %d\n# friends found: %d' %(num_friends, len(friend_set))) 
    
    return(friend_set)

######################################################################################    
########################## II. Processing Functions ################################## 
def extract_data(response, url):
    """INPUT: response -- the data given by response.content() from Requests module.
    OUTPUT: 1. data_dict -- the data dictionary of desired data.
            2. appended reviews file. See [append_reviews_txt()]
    EXTERNAL function"""
    data_dict={}
    data_dict['url']=[url]
    soup=BeautifulSoup(response)
    user_id=re.search('userid=(\S+)',url).group(1)
    ##FUNCTION START
    #Corrupted website?
    check1=soup.find("li", class_='miniOrange')
    if check1 is not None:
        print("\n!!!!!Yelp gave a corrupted website!!!!!!")
        return("Bad soup")
    
    data_dict['user_id']=user_id
    #User location:
    if soup.find('h3', class_='user-location alternate') is not None:
        data_dict['Location']=soup.find('h3', class_='user-location alternate').getText()
    if soup.find('h3', class_='user-location alternate') is None:
        data_dict['Location']=np.nan
    
    #Friend and Review Count:
    for grab in ['friend-count', 'review-count']:
        if soup.find('li', class_=grab) is not None:
            data_dict[grab]=int(re.search('\d+',str.strip(soup.find('li', class_=grab).getText())).group())
        if soup.find('li', class_=grab) is None:
            data_dict[grab]=np.nan
     
    #Elite Status:
    data_dict['elite_num']=len(soup.findAll('span', class_='elite-badge'))
    
    #Bizs Reviewed:
    if data_dict['review-count']>1:
        Master_reviews_list=fetch_user_reviews(user_id, data_dict['review-count'])
        rvwd_biz_url_list=[review_data.find('a', class_='biz-name').attrs['href'] for review_data in Master_reviews_list]
        rvwd_biz_date_list=[str.strip(review_data.find('span', class_='rating-qualifier').getText()) for review_data in Master_reviews_list]
        data_dict['bizReviewed']=rvwd_biz_url_list
        data_dict['bizRvwDate']=rvwd_biz_date_list
    if data_dict['review-count']==1:
        data_dict['bizReviews']=np.nan
        data_dict['date_rvwd']=np.nan
    #Biz Reviewed Ratings
    """
    rvwd_biz_rating_list=[]
    for review_data in Master_reviews_list:
        rvwd_biz_rating_list.append(int(re.search('(\d+)',review_data.find('i', class_='star-img').attrs['title']).group(1)))
    data_dict['given_stars']=rvwd_biz_rating_list
    """
    
    #Friends
    if data_dict['friend-count']>0:
        friend_list=fetch_user_friends(user_id, num_friends=data_dict['friend-count'])
        data_dict['friendIDs']=friend_list
    if data_dict['friend-count']==0:
        data_dict['friend-count']=0
    
    ##FUNCTION END   
    return(data_dict)
       

########################################################################################        
############################ III. Convenience Functions ################################            
def make_json(file_name='yelp_user_data.json'):
    """file_name -- the file where scraped reviews will be saved.
    EXTERNAL function. """
    if os.path.isfile(file_name)==False:
        with open(file_name, 'w') as f:
            json.dump('[', f)
        
def make_update_df(data_dict, file_name="SanDiego_biz_addendum.csv"):
    empty_data_dict={}
    for key in data_dict.keys():
        empty_data_dict[key]=[]
    pd.DataFrame(empty_data_dict).to_csv(file_name, index=False) 
    
def Pickle(data,file_name='business_addendum.pkl'):
        with open(file_name, 'wb') as f:
            pkl.dump(data, f)
        print("Downloaded JSON data pickled to [%s]" %file_name)   
        
def eat_pickle(file_name='business_addendum.pkl'):
    with open(file_name, 'rb') as f:
        return(pkl.load(f))        

def write_count(count, file_name="count.txt", start_count='0' ):
    if os.path.isfile(file_name) == False:
        print("Creating new count file: [%s]" %file_name)
        with open(file_name, 'w') as f:
            f.write(start_count)
            
    if os.path.isfile(file_name):
        with open(file_name, 'w') as the_file:
            the_file.write(str(count))
 
def read_count(file_name='count.txt'):
    with open(file_name) as f:
        count=f.readline()
        return(int(count))    
        
def counter_reset():
    answer='the cake is a lie!'
    while (answer!='Y') & (answer!='N'):
        answer=input("Would you like to reset the counter to 0? [Y/N]: ")
    if answer == "Y":    
        write_count(0)
        print("Counter reset")      

def step_display(i):
    if i%50==0:
        print("On number: %s" %i)

def wait():
    wait_time=int(rnd.uniform(low=1,high=5))
    print("\nPausing for: %d seconds..." %wait_time)
    time.sleep(wait_time)
    #print('seconds: [%d%%]\r' %seconds_elapsed)
    print("--"*20)        

def read_json_as_text(reviews_file='biz_reviews_collection.json'):    
    with open(reviews_file) as f:
        user_data=f.read()
    
    try:
        data_dict=json.loads(user_data)
    except Exception:
        user_data2=re.sub("}{","},{" ,user_data)
        data_dict=json.loads(user_data2)
    return(data_dict)    
    """
    with open(    'biz_reviews_collection_fixed.json', 'w') as f:
        f.write(user_data2)
    reviews_file='biz_reviews_collection_fixed.json'
    with open(reviews_file, 'r').read() as f:
        user_data=json.loads(f)  
    """
    
def extract_user_ids(reviews_list):
    users_list=[]
    for i in range(0,len(reviews_list)):
        reviews_dict=reviews_list[i]
        entries_list=reviews_dict[list(reviews_dict.keys())[0]]
        if len(entries_list)>0:
            reviews_id_list=[re.sub("^user_id:", "",review['id']) for review in entries_list]
            users_list.extend(reviews_id_list)
    print("Total # of entries found: [%d] " %len(users_list))
    unique_users_list=list(set(users_list))
    print("\nTotal # of unique users found: [%d] " %len(unique_users_list))
    return(unique_users_list)    
 
def construct_user_urls(unique_users_list, base_url='http://www.yelp.com/user_details?userid='):
    users_url_list=[base_url+user_id for user_id in unique_users_list]
    return(users_url_list)
  
def load_yelp_user_urls(count ,file_name='yelp_users_url.pkl'):  
    if os.path.isfile(file_name)==True:
        print("Loading data...")
        with open(file_name, 'rb') as f:
            users_url_list=pkl.load(f)[count:]

    if os.path.isfile(file_name)==False:
        #if the pickle doesn't exist yet, then make one
        print("\nCouldn't find Pickled User URLs so Loading Original JSON Data")
        reviews_list=read_json_as_text()
        unique_users_list=extract_user_ids(reviews_list)
        users_url_list=construct_user_urls(unique_users_list)
        with open(file_name, 'wb') as f:
            pkl.dump(users_url_list, f)
        print("\nPickled Yelp User URLs to [%s]" %file_name)    
    print("Data loaded")
    print("--"*20)
    return(users_url_list) 
    
def add_bad_soup(url, file_name='bad_soup_urls.txt'):
    if os.path.isfile(file_name):
        with open(file_name, 'a') as f:
            f.write(','+url)
    if os.path.isfile(file_name)==False:
        with open(file_name, 'w') as f:
            f.write(url)
    print("\nBad soup's URL recorded in: [%s]" %file_name)       
    
def give_rndm_userAgent(user_agent_list):
    rnd_agent=np.random.choice(user_agent_list,1)[0]
    user_agent={'User-agent':rnd_agent}
    return(user_agent)
    
#######################################################################################        
########################### IV. MAIN Function ########################################
def main():
    data_path="c:/users/gene/documents//duke/dropbox/gene/yelp_scrapping"
    os.chdir(data_path)
    
    counter_reset()
    count=read_count()
    print("--"*20)
    users_url_list=load_yelp_user_urls(count)
    print("--"*20)
    make_json()
    
    for (i,url) in zip(range(count, len(users_url_list)),users_url_list):
        print("--"*40)
        print("--"*40)
        print('  '*15,url)
        step_display(i)
        response=fetch_website(url)
        Pickle(response, file_name='dwnld_user_profile.pkl')
        #response=eat_pickle('dwnld_user_profile.pkl')
        if response=='Skip':
            pass
        if response !='Skip':
            data_dict=extract_data(response, url)
            if data_dict!='Bad soup':
                with open('yelp_user_data.json', 'a') as f:
                    json.dump(',', f, indent=0)
                    json.dump({data_dict['user_id']:data_dict}, f, indent=2)
            if data_dict=='Bad soup':
                add_bad_soup(url)

        write_count(i)
        #if int(i)%3==0:
        wait()

if "__main__"==__name__:
    main()