#TODO: clean up imports
import lib.db as db
from lib.db import Farm, Batch, Recipe, Cultivar
import peewee
import unittest

import pdb

class TestBase(unittest.TestCase):
    def setUp(self):
        self.db = peewee.SqliteDatabase('farm_ops_test.db')
        db.proxy.initialize(self.db)
        self.db.create_tables([Farm, Recipe, Cultivar, Batch], safe=True)
        Farm.insert(name='test_site').on_conflict_replace().execute()

    def tearDown(self):
        self.db.drop_tables([Farm, Recipe, Cultivar, Batch])