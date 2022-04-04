#!/usr/bin/env python
import pika
import pdb

class Publisher():
    def __init__(self):
        #TODO: move to ENV var
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost')) 
        self.channel = self.connection.channel()

    def declare_queue(self, name):
        self.channel.queue_declare(queue=name)
    
    def enqueue(self, payload):
        self.channel.basic_publish(
            exchange='', 
            routing_key='hello', 
            body=payload
        )

        print(f"Enqueued task: {payload}")

        #TODO: can we use `with` to ensure that the close() method is always called?
        self.connection.close()