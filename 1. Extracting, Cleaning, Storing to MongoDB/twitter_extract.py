__author__ = 'Harsh'

import tweepy
import json
from pymongo import MongoClient
import re

#extracting twitter data
def twitter_extract():
    auth=tweepy.OAuthHandler('vXcbdBd5ajXqTVwdJEDknvPg1','wzyVagAsvOUVCmJ2JuZ9iLy9AsDogqhyJM521qDMtVoL49ZO9H')
    auth.set_access_token('982652708736741376-tHvZwdoPo7XngUBSwvKuzvSAhqfe70Q','RJXDG3Z97uPlmJwmcVFfpEDEnyhrzKvqDozGlLmvDLx1V')

    api=tweepy.API(auth,wait_on_rate_limit=True)
    dict={}
    l=['Canada','Dalhousie University','University','Halifax','Canada Education']
    for i in l:
        canada_res=tweepy.Cursor(api.search,q=i,lang='en').items(1000)
        list=[]
        for j in canada_res:
             list.append(((j.text.encode('ascii','ignore')).decode('ascii'),j.retweet_count,str(j.created_at),j.user.location,j.coordinates))
        dict[i]=list

    with open ('main_data_twitter.json','w',) as f:
        json.dump(dict,f)
twitter_extract()

# creating json file for mongodb, cleaning data, creating text file for spark processing
def json_creation():
    main_dict=[]
    with open('main_data_twitter.json','r') as f:
        f2=open('twitter_texts.txt','w')
        dict=json.load(f)
        keys=dict.keys()
        for i in keys:
            for j in dict[i]:
                d={}

                #cleaning data (removing hyperlinks and special characters and putting space between all words)
                j[0]=re.sub('[^A-Za-z0-9]+', ' ', j[0])
                j[0]=re.sub(r'http\S+', ' ', j[0])
                j[0]=j[0].lower()

                #creating dict to save it to mongodb
                d['text']=re.sub(r'http\S+', ' ', j[0])
                d['retweet']=j[1]
                d['created at']=j[2]
                d['user location']=j[3]
                d['coordinates']=j[4]
                main_dict.append(d)    # adding to a main dictionary

                #writig to a text file for pyspark processing
                f2.write(d['text'])   # appending to a twitter_text file
        f2.close()
        f1=open('main_data_news.json','w')    # opeining main dict to save the data
        json.dump(main_dict,f1)
        f1.close()
json_creation()  # fetch data from the list ( which containts twitter data ) and saves it to a dictionary and a text file
                 # for further use.


#saving the dictionary to mongodb
def connect():
    connection = MongoClient('localhost', 27017)
    database=connection["twitter"]
    data=database['data']
    f1=open('main_data_news.json','r')    # opening main dict to save the data
    main_dict=json.load(f1)
    print(type(main_dict))
    print(connection.list_database_names())
    data.insert_many(main_dict)
    #data.insert_many(dict)
    f1.close()
connect()   # fetches the created dictionary to store data in mongodb
