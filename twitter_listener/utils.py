import tweepy
from dotenv import load_dotenv
import os


def connect_to_twitter_oauth():
    load_dotenv()
    auth = tweepy.OAuthHandler(os.getenv('CONSUMER_KEY'), os.getenv('CONSUMER_SECRET'))
    auth.set_access_token(os.getenv('ACCESS_TOKEN'), os.getenv('ACCESS_SECRET'))
    api = tweepy.API(auth)
    return api