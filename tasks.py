from concurrent.futures import process
from celery import Celery
from lot_handler import LotHandler
from publisher import Publisher
import json
import pdb

app = Celery('tasks', broker='amqp://localhost')
publisher = Publisher()

# @app.task
# def process_schedule(file):
#     Ingester("./crop_plan.xlsx").trigger_batch_tasks()

@app.task
def test(arg):
    print('gets here')
    print(arg)


@app.task
def enqueue_tasks(lot):
    payloads = LotHandler(json.loads(lot)).batch_payloads
    # publisher = Publisher().declare_queue(f"batch_queue_{self.scheduled_date}")
    # publisher.enqueue(self.batches())

    #TODO: add logger lines to allow batch tracking