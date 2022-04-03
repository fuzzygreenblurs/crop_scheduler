#!/usr/bin/env python
import pika
from datetime import datetime

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')

channel.basic_publish(
    exchange='', 
    routing_key='hello', 
    body=f"{datetime.now().strftime('%H:%M:%S')}: Hello World!"
)

print(" [x] Sent 'Hello World!'")

connection.close()