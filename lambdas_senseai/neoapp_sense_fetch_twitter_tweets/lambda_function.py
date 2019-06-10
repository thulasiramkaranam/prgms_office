import json
import psycopg2
import os
import logging
import requests
import base64
import datetime
from datetime import datetime as dtt
from datetime import timedelta
logger = logging.getLogger()
logger.setLevel(logging.INFO)


logger.info("connected new code")
def lambda_handler(event, context):
    screenames = ["@amazon", "@BestBuy", "@Kroger", "@Target", "@HomeDepot", "@Lowes",                       "@Walgreens"]
    # TODO implement
   
    conn = psycopg2.connect(host='neo-app-vso.c8yss8lfzn7r.us-east-1.rds.amazonaws.com' ,port=5432,             dbname='senseai',user='neo_app_vso_root',password = 'Bcone,12345')
    cur = conn.cursor()
    insert_query = "INSERT INTO twitter_tweets_by_persona VALUES(%s,%s,%s) on conflict (tweet_date,                  tweeted_by) do nothing"
    for name in screenames:
        client_key = 'rgdRIt8STg1p4MZFgZeiM2vJm'
        client_secret = 'lz309GYU3IVT8uPSe7Me4wcRsehLyAazB7YtAJudVP3B0nxbf6'
        key_secret = '{}:{}'.format(client_key, client_secret).encode('ascii')
        b64_encoded_key = base64.b64encode(key_secret)
        b64_encoded_key = b64_encoded_key.decode('ascii')
        base_url = 'https://api.twitter.com/'
        auth_url = '{}oauth2/token'.format(base_url)
        auth_headers = {
            'Authorization': 'Basic {}'.format(b64_encoded_key),
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
                }
        auth_data = {
                'grant_type': 'client_credentials'
            }
        auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)
        access_token = auth_resp.json()['access_token']
        search_headers = {
            'Authorization': 'Bearer {}'.format(access_token)    
            }

        url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name={0}&count=10'
        url = url.format(name)
        search_resp = requests.get(url, headers=search_headers)
        print(search_resp.text)
        print("after status code")
        print(name)
        tweets = json.loads(search_resp.text)
        for tweet in tweets:
            print(tweet)
            print("---------------------------------------------------------")
            date_obj = str(datetime.datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            tweeted_epochtime = int((dtt.strptime(date_obj, "%Y-%m-%d %H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
            message = tweet['text']
            screename = name
            
            cur.execute(insert_query, (tweeted_epochtime,screename,message))
    conn.commit()
    # print("inserted the records")


