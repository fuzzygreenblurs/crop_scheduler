#!/usr/bin/env python
import pika
import pdb
import json

class Consumer():
    def __init__(self, key):
        #TODO: move to ENV var
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='batch_schedule', exchange_type='direct')

        self.key = key

        self.channel.queue_declare(queue=key)
        self.channel.queue_bind(
            exchange='batch_schedule',
            queue=key,
            routing_key=key
        )

    def consume(self, callback=None):
        self.channel.basic_consume(queue=self.key, on_message_callback=self.basic_callback, auto_ack=True)
        self.channel.start_consuming()

    def basic_callback(self, ch, method, properties, body):
        print(f"{method.routing_key}:{json.loads(body)}\n")

# Consumer('batch-queue-2022-12-26 00:00:00').consume()
# Consumer('batch-queue-2022-12-25 00:00:00').consume()