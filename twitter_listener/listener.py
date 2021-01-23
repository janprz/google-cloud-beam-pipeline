import tweepy
import os
from dotenv import load_dotenv


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        self.connect()

    def on_status(self, status):
        print(status.text)

    def connect(self):
        load_dotenv()
        auth = tweepy.OAuthHandler(os.getenv('CONSUMER_KEY'), os.getenv('CONSUMER_SECRET'))
        auth.set_access_token(os.getenv('ACCESS_TOKEN'), os.getenv('ACCESS_SECRET'))
        self.api = tweepy.API(auth)
