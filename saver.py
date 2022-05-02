from kafka import KafkaConsumer
import json
import pymongo

consumer = KafkaConsumer ('testTopic',bootstrap_servers = ['localhost:9092'], \
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),\
    enable_auto_commit=True)
li = []
for message in consumer:
    li.append(message.value)
    if(len(li)==15000):
        break
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Ass4"]
mycol = mydb["try1"]
x = mycol.insert_many(li)