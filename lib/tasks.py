from celery import Celery
from lib.lot import Lot
from lib.producer import Producer
import json

# TODO: add backend to rabbit for log storage
app = Celery('tasks', broker='amqp://localhost')

@app.task
def enqueue_batches(lot_data):
    print(json.loads(lot_data))
    lot = Lot(lot_data=json.loads(lot_data))
    Producer(f"batch-queue-{lot.date}").produce(
        json.dumps(lot.batch_payloads, indent=4, sort_keys=True, default=str)
    )

#TODO: add logger lines to allow batch tracking