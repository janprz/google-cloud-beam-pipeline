import tweepy
import os
import json
import boto3
from dotenv import load_dotenv


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        load_dotenv()
        self.api = None
        self.topic_arn = None
        self.sns_client = None
        self.initialize_twitter_api()
        self.initialize_sns_client()

    def on_status(self, status):
        print(status.text)
        publish_object = {'text': status.text}
        response = self.sns_client.publish(
            TopicArn=self.topic_arn,
            Message=json.dumps(publish_object),
            Subject='twitter_text')
        print(response['ResponseMetadata']['HTTPStatusCode'])

    def initialize_twitter_api(self):
        auth = tweepy.OAuthHandler(os.getenv('CONSUMER_KEY'), os.getenv('CONSUMER_SECRET'))
        auth.set_access_token(os.getenv('ACCESS_TOKEN'), os.getenv('ACCESS_SECRET'))
        self.api = tweepy.API(auth)

    def initialize_sns_client(self):
        self.topic_arn = os.getenv('TOPIC_ARN')
        self.sns_client = boto3.client(
            'sns',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
            region_name=os.getenv('REGION_NAME')
        )

