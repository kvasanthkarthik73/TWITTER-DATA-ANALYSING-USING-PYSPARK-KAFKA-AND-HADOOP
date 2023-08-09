import tweepy
from tweepy.streaming import StreamListener

from kafka import KafkaProducer
import json

# Twitter API keys and tokens
consumer_key = "se1iaPGsJiQInSLj4n1s4N3Pv"
consumer_secret = "tquHVz65eJAQTR5TvShoU8fzALYh0KwfTFOy9OmnBiVHpLcFEp"
access_token = "849614034399670273-tgFouYzJAvJRw5wbaTuLUeoJyhSTcQk"
access_token_secret = "53wbvMuR381iYw1RkjQKxi6QokDwEmADILPW2YjwJh4pk"

# Kafka producer configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "tweetz"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

class MyStreamListener(StreamListener):
    def on_status(self, status):
        tweet = {
            "text": status.text,
            "user": status.user.screen_name,
            "timestamp": str(status.created_at)
        }
        producer.send(kafka_topic, tweet)
        print("Sent tweet to Kafka:", tweet)

if __name__ == "__main__":
    try:
        stream_listener = MyStreamListener()
        twitter_stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
        twitter_stream.filter(track=["INDvsPAK"])
    except Exception as e:
        print("An error occurred:", str(e))


