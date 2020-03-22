import paho.mqtt.client as mqtt
import json
import pymongo

# mongoDB server link
db_host_mongo_db_atlas = "mongodb+srv://secterica:iotlvivua2020@sectericacluster-dkxim.mongodb.net/test?retryWrites=true&w=majority"

db_name = 'HeySmell'
collection_name = 'sensors_data'

# link on public mqtt broker server we use right now (from HiveMQ)
broker = "broker.hivemq.com"

# we subscribe on all topics from topic secterica/heysmell/
topic_subscribe = "secterica/heysmell/#"

# topic to get info
topic_info = "secterica/heysmell/sensors_info"

# optional
topic_control = "secterica/heysmell/control"

# just random name to define us
# when we`ll use not public broker, we might create user also with some password
client_name = "heysmell"


# here are actions, which are done by script, when we get message from topic
# actions: inserts message in mongoDB collection, if it`s in json format
def on_message(client, userdata, message):
    returned_message = str(message.payload.decode("utf-8"))
    print("message topic:", message.topic)
    print("message:", returned_message)
    if message.topic == topic_info:
        insert_message_into_mongodb(returned_message)


# on_log, on_connect and on_disconnect:
# just some logs for us to understand what`s happening better while connecting and while disconnecting
def on_log(client, userdata, level, buf):
    print("log: ", buf)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print('Successfully connected to broker', broker)
    else:
        print('Bad connection to broker' + broker + ', returned code' + str(rc))


def on_disconnect(client, userdata, flags, rc):
    print('Disconnected from broker' + broker + ', returned code' + str(rc))


# inserts message in mongoDB collection
def insert_message_into_mongodb(message):
    record = json.loads(message)
    collection.insert_one(record)
    print('message saved to collection ' + collection_name + ' in database ' + db_name)


# here we connect to mongoDb and create database and collection in it if it doesnt exist
# if db with such name is already in use it will just connect to it
connection = pymongo.MongoClient(host=db_host_mongo_db_atlas)
database = connection[db_name]
collection = database[collection_name]
print("Connected to " + db_name + '\n')

# creating new instance for client
client = mqtt.Client(client_name)

# overriding functions for logging
client.on_log = on_log
client.on_connect = on_connect
client.on_disconnect = on_disconnect

# overriding function on_message
client.on_message = on_message

# connecting to broker and subscribing to topics
client.connect(broker)
client.subscribe(topic_subscribe)
print("Subscribed to topic", topic_subscribe)

# starting loop
while True:
    client.loop()

    #
    # if statement to disconnect here
    #
