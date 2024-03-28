import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "5S3TK2jQpm8HXyyweZ5gpxJap" 
    access_secret = "UJ2ZZ4RF71LcnXF3sSZWnaPY8fMFYVIwn5jvRuOjd7QEt49zEA" 
    consumer_key = "1773243613159555072-VACgMlLX7v6wOAS0IQ4v8zuppLTueM"
    consumer_secret = "dp70cTECqmlZ3euQQHmgqB3BNU5UloYyMqZKp81vsM7s5"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('s3://sahas-airflow-one/refined_tweets.csv')
