import os
from invoke import task
from dotenv import load_dotenv
from lib.consumer import Consumer 
from lib.ingester import Ingester

load_dotenv()

# invoke tasks in the command line: $ invoke dummy
@task
def dummy(c):
    print("can invoke successully!")

@task
def startRabbit(c):
    os.system('docker run -it --rm --hostname my-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3.8-management')

@task
def startCelery(c):
    os.system(f'celery -A lib.tasks worker --loglevel=info --concurrency={os.getenv("CELERY_CLUSTER_SIZE")}')

@task
def consume(c, binding_key):
    # for example: $ invoke consume 'batch-queue-2022-12-26 00:00:00'
    Consumer(binding_key).consume()

@task
def runTests(c):
    os.system('python3 -m unittest ./test/test_ingester.py')
    os.system('python3 -m unittest ./test/test_lot.py')

@task
def ingest(c, file_path):
    os.system('python3 ./lib/db.py')
    Ingester(file_path).process_lots_data()









'''
Notes:
- stand up both dev and test environments
    - seed the data for Farm (and maybe Cultivar)

    - for dev:
        - start celery worker pool (3 workers)
        - start consumer pool (3 workers). include workers for:
            - consuming jobs (maybe add some simple logic for prioritizing queues)
                - upon consuming, push to a file for consumption. this file can quickly show the order of processing - maybe batches.logs?
            - include workers for monitoring
                - for ERROR type logs, these workers should push to a separate file ERROR.logs
                - for INFO type logs, these workers should push to a separate file INFO.logs

        - option to do a live test at scale? (handle 100s of spreadsheets at once)
        - option to open the RabbitMQ monitoring tool 

    - for test:
        - start worker pool
        - give the user the option to run all the tests in one command
'''