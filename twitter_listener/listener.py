import tweepy
import os
import json
import boto3
from dotenv import load_dotenv
from google.cloud import pubsub_v1
import datetime
import time


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        load_dotenv()
        self.api = None
        self.topic_path = None
        self.pubsub_client = None
        self.initialize_twitter_api()
        self.initialize_pubsub_client()

    def on_status(self, data):
        print(data)
        self.send_to_pubsub(data._json)

    def send_to_pubsub(self, data):
        if data['lang'] == 'en':
            self.pubsub_client.publish(self.topic_path,data = json.dumps({
                "id": data["id"],
                "time_stamp": datetime.datetime.fromtimestamp(time.mktime(time.strptime(data["created_at"], "%a %b %d %H:%M:%S +0000 %Y")))
                    .strftime('%Y-%m-%d %H:%M:%S'),
                "text": data['text'],
                "username": data['user']['name'],
                "n_followers": int(data['user']['followers_count'])
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))

    def initialize_twitter_api(self):
        auth = tweepy.OAuthHandler(os.getenv('CONSUMER_KEY'), os.getenv('CONSUMER_SECRET'))
        auth.set_access_token(os.getenv('ACCESS_TOKEN'), os.getenv('ACCESS_SECRET'))
        self.api = tweepy.API(auth)

    def initialize_pubsub_client(self):
        self.pubsub_client = pubsub_v1.PublisherClient()
        self.topic_path = self.pubsub_client.topic_path(os.getenv('GC_PROJECT'),os.getenv('PUBSUB_TOPIC'))

