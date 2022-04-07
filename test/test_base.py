import os
import lib.db as db
import peewee
import unittest
from dotenv import load_dotenv
from lib.db import proxy, Farm, Batch, Recipe, Cultivar

load_dotenv()
class TestBase(unittest.TestCase):
    models = [Farm, Recipe, Cultivar, Batch]

    def setUp(self):
        self.db = peewee.SqliteDatabase(os.getenv('TEST_DB'))
        proxy.initialize(self.db)
        self.db.create_tables(self.models, safe=True)
        Farm.insert(name=os.getenv('TEST_FARM_NAME')).on_conflict_replace().execute()

    def tearDown(self):
        self.db.drop_tables(self.models)