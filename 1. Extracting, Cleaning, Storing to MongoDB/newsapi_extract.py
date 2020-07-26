__author__ = 'Harsh'
from newsapi import NewsApiClient
import json
import re
from pymongo import MongoClient


#extract news data, cleaning, storing to a dict for further use
def news_extract():
    main_dict=[]
    api=NewsApiClient(api_key='0fc0e9b54d5d4389b807d66a86fcee50')
    l=['Canada','Dalhousie University','University','Halifax','Canada Education']

    f=open('news_texts.txt','w')
    for i in l:
        data=api.get_everything(q=i)
        for j in data['articles']:
            dict={}
            #print("content"+j['content'])
            #cleaning data (eliminating all special characters and converting string to lowercase)
            j['title']=str(re.sub('[^A-Za-z0-9]+', ' ', j['title'])).lower()
            j['description']=str(re.sub('[^A-Za-z0-9]+', ' ', j['description'])).lower()

            #creating a dict to save to mongodb
            dict['title']= j['title']
            dict['description']=j['description']
            dict['author']=j['author']
            dict['content']=j['content']
            main_dict.append(dict)

            #writing to a text file
            f.write(j['title']+' ')
            f.write(j['description']+' ')

            #because all news dont contain 'content'
            try:
                f.write(j['content'])
            except:pass
    f=open('main_data_news.json','w')
    json.dump(main_dict,f)
news_extract()




#saving the dictionary to mongodb
def connect():
    connection = MongoClient('localhost', 27017)
    database=connection["news"]
    data=database['data']

    f1=open('main_data_news.json','r')    # opeining main dict to save the data
    main_dict=json.load(f1)
    print(type(main_dict))
    print(connection.list_database_names())
    data.insert_many(main_dict)
    f1.close()
connect()

