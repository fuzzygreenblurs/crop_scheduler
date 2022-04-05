#!/usr/bin/env python
import pika
import pdb

class Producer():
    def __init__(self, key):
        #TODO: move to ENV var
        self.key = key
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='batch_schedule', exchange_type='direct')
        
        self.channel.queue_declare(queue=self.key)
        self.channel.queue_bind(
            exchange='batch_schedule', 
            queue=self.key, 
            routing_key=self.key
        )
    
    def produce(self, payload):
        # TODO: make queues durable and add QoS
        self.channel.basic_publish(
            exchange='batch_schedule',
            routing_key=self.key,
            body=payload
        )
        
        print(f"Enqueued task: {payload}")
        self.connection.close()
        