import listener
import tweepy
import logging


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    stream_listener = listener.TwitterStreamListener()
    stream = tweepy.Stream(auth=stream_listener.api.auth, listener=stream_listener)
    hashtags = ['#Cyberpunk2077']
    stream.filter(track=hashtags)