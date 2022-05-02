from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = ""
access_token_secret =  ""
consumer_key =  ""
consumer_secret =  ""

class StdOutListener(StreamListener):
    def __init__(self):
        self.co = 0
    def on_data(self, data):
        producer.send_messages("testTopic", data.encode('utf-8'))
        self.co+=1
        return True
    def on_error(self, status):
        print ("Error :",status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["covid","covidvaccine"])