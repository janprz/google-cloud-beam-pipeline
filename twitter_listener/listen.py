import listener
import tweepy


if __name__ == '__main__':
    stream_listener = listener.TwitterStreamListener()
    stream = tweepy.Stream(auth=stream_listener.api.auth, listener=stream_listener)
    hashtags = ['#Cyberpunk2077']
    stream.filter(track=hashtags)
    # stream_listener.test_sns_publishing()