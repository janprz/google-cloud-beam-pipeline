import boto3
import os
from dotenv import load_dotenv
import time
import json


def receive_message():
    load_dotenv()
    sqs_client = boto3.client(
        'sqs',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
        region_name=os.getenv('REGION_NAME')
    )
    response = sqs_client.receive_message(
        QueueUrl=os.getenv('QUEUE_URL'),
        AttributeNames=['All'],
        MessageAttributeNames=[
            'All',
        ],
        MaxNumberOfMessages=5,
        VisibilityTimeout=10,
        WaitTimeSeconds=10
    )
    tweets = []
    for message in response['Messages']:
        text = json.loads(json.loads(message['Body'])['Message'])['text']
        tweets.append(text)
    return tweets
