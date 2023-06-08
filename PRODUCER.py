import tweepy
from kafka import *
import configparser
from collections import *
import requests
import time
import pandas as pd
import numpy as np
import logging
import json
from decouple import *
import pymongo
from pymongo import *

###LETTURA FILE CONFIG.INI-------------------------------------------------------------------------------------------
config = configparser.ConfigParser()
config.read('config.ini')
api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']
bearer_token = "AAAAAAAAAAAAAAAAAAAAALHqnAEAAAAAeMfAe9WjCSTbJ5ynRDgtZd9CFwI%3DRPuFwoieYJqIpKIh17yvhoUx0GuI5HH6UDYfxYwIHVP4gBJmWt"

###CONFIGURAZIONE CLIENT MONGODB-------------------------------------------------------------------------------------------
#client = pymongo.MongoClient("mongodb://mongodb:27017/")
#db = client["twitter"]
#collection1 = db["POST"]
#logging.basicConfig(level=logging.INFO)

###BOOTSTRAP SERVER KAFKA-------------------------------------------------------------------------------------------
bs = 'kafka1:9092'
bs1 = 'localhost:9092'

###PRODUCER KAFKA-------------------------------------------------------------------------------------------
producer = KafkaProducer(bootstrap_servers = bs)

###PAROLE DA INSERIRE COME RULES-------------------------------------------------------------------------------------------
keywords = ["Tatum", "Jokic", "Doncic", "Embiid"]
#keywords = "Jokic"

###TOPIC DI KAFKA-------------------------------------------------------------------------------------------
topic_name = 'twitterStream'

###AUTH-------------------------------------------------------------------------------------------
def twitterAuth():
    authenticate = tweepy.OAuthHandler(api_key, api_key_secret)
    authenticate.set_access_token(access_token, access_token_secret)
    authenticate.secure = True
    #API OBJ
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api

####CREAZIONE BOT PER LA RICERCA-------------------------------------------------------------------------------------------
class MyStream(tweepy.StreamingClient):
    
    ###AVVISSO AVVIO
    def on_connect(self):
        print("CONNECTED!")

    #INVIO AL PRODUCER
    def on_data(self, rdata):
        #logging.info(rdata)
        tweet = json.loads(rdata)
        #tweet_post = {'content': tweet['data']['text']}
        #collection1.insert_one(tweet_post)
        
        if tweet['data']:
            data = {
                'message': tweet['data']['text'].replace(',', '')
            }
            producer.send(topic_name, value=json.dumps(data).encode('utf-8'))
            #print(data)
        return True
    
    def on_error(status_code):
        if status_code == 420:
            return False
        
    def start_streaming(self, keywords):
        for term in keywords:
            self.add_rules(tweepy.StreamRule(term))
        self.filter()
        
if __name__ == '__main__':
    twitter_stream = MyStream(bearer_token)
    twitter_stream.start_streaming(keywords)