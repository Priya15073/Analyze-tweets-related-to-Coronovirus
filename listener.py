from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
import json
import configure

client = SimpleClient("localhost:9092")
producer = SimpleProducer(client)
consumer_key = configure.consumer_key
consumer_secret = configure.consumer_secret
access_token = configure.access_token
access_token_secret = configure.access_secret

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        data=json.loads(data)
        #print(data)
        user_id=data['user']['id_str']
        producer.send_messages('covid',(user_id).encode('utf-8'))
        print(user_id)
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
  l = StdOutListener()
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  stream = Stream(auth, l)
  stream.filter(track=['#covid19'],languages=["en"])
