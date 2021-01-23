import boto3
import os
from dotenv import load_dotenv
import time
import json


load_dotenv()
sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
            region_name=os.getenv('REGION_NAME')
        )

while True:
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
    # print(sqs_client.list_queues())
    print('Received a message')
    for message in response['Messages']:
        print(json.loads(json.loads(message['Body'])['Message'])['text'])
        # print(type(json.loads(message['Body'])))
    time.sleep(10)
