from concurrent.futures import process
from celery import Celery
from lot import Lot
from producer import Producer
import json
import pdb

app = Celery('tasks', broker='amqp://localhost')

@app.task
def test(arg):
    print('gets here')
    print(arg)


@app.task
def enqueue_batches(lot_data):
    lot = Lot(json.loads(lot_data))
 
    Producer(f"batch-queue-{lot.date}").produce(
        json.dumps(lot.batch_payloads, indent=4, sort_keys=True, default=str)
    )

    #TODO: add logger lines to allow batch tracking
