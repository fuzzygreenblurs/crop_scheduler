from concurrent.futures import process
from celery import Celery
from lot import Lot
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
def enqueue_batches(lot_data):
    lot = Lot(json.loads(lot_data))
    publisher = Publisher()
    
    publisher.declare_queue(f"batch_queue_{lot.date}")
    publisher.enqueue(lot.cultivar_name) # this works
    publisher.enqueue(json.dumps(
        lot.batch_payloads, 
        indent=4, 
        sort_keys=True, 
        default=str)
    ) 

    pdb.set_trace()

    #TODO: add logger lines to allow batch tracking
