import os
from dotenv import load_dotenv
import time
import json
from google.cloud import pubsub_v1


def callback(message):
    print(json.loads(message.data))
    message.ack()


def receive_message():
    load_dotenv()
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(os.getenv('GC_PROJECT'), os.getenv('PUBSUB_SUBSCRIPTION'))
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    timeout = 5.0
    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()


if __name__ == '__main__':
    receive_message()