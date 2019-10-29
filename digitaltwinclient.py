import sqlite3
from sqlite3 import Error
import paho.mqtt.client as mqtt
import datetime

broker = "192.168.1.163"
port = 1883
topic = "outTopic"

database = "/home/pi/digitaltwin_db"
tableName = "DIGITAL_TWIN_EVENTS"

transactionID ="TRANACTION_ID"
typeLabel = "TRANSACTION_TYPE"
infoLabel = "TRANSACTION_INFO"
dateLabel = "TRANSACTION_DATE"

insertString = "insert into {} ({},{},{}) values ({},{},{})"

def create_connection(db_file):
    conn=None
    try:
        conn=sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn

def create_transaction(conn, transaction):
    sql = "insert into {} ({},{},{}) values ({},{},{})"
    print("get cursor")
    cur = conn.cursor()
    print("execute transaction")
    cur.execute(sql, transaction)

#TRANACTION_ID INTEGER, TRANSACTION_TYPE TEXT, TRANSACTION_INFO TEXT, TRANSACTION_DATE TEXT
#INSERTS TRANSACTION INTO DIGITAL_TWIN_DB.DIGITAL_TWIN_LOG TABLE
def insert_to_table(payloadString):
    info = ""
    transactionType=""
    if "Temperature" in payloadString:
        transactionType = "Temperature Event"
        info = payloadString.replace("Temperature","")
    elif "DoorClosed" in payloadString or "DoorOpen" in payloadString:
        transactionType = "Door Event"
        info = payloadString
    elif "lightOn" in payloadString or "lightOff" in payloadString:
        transactionType = "Light Event"
        info = payloadString
    elif "Connected" in payloadString:
        transactionType = "Connection Event"
        info = payloadString
    else:
        transactionType = "Message Event"
        info = payloadString

    #print("transactionType: %s transactionInfo: %s transcationDate: %s" % (transactionType, info, datetime.datetime.now()))
    datetimestr=datetime.datetime.now()
    print(insertString.format(tableName, typeLabel, infoLabel, dateLabel, transactionType, info, datetime.datetime.now()))

    conn=create_connection(database)
    print("connection created")
    with conn:
        print("creating transaction table")
        transaction = (tableName, typeLabel, infoLabel, dateLabel, transactionType, info, datetimestr)
        print("creating transaction")
        create_transaction(conn, transaction)
        print("transaction created")

    
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(topic)

# The callback for when a PUBLISH message is received from the server.
#print("%s %s"% (msg.topic,msg.payload.decode("utf-8")))
def on_message(client, userdata, msg):
    insert_to_table(msg.payload.decode("utf-8"))
    

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, port, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
