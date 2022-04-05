from unittest import mock
from lib.ingester import Ingester
from test.test_base import TestBase
from lib.db import *
from datetime import datetime
import pandas as pd

class TestIngester(TestBase):

    @mock.patch.object(Ingester, 'read_crop_schedule')
    @mock.patch.object(Ingester, 'read_recipes')
    def test_cultivar_and_recipe_inserts(self, read_recipes, read_crop_schedule):
        self.assertEqual(Cultivar.select().count(), 0)
        self.assertEqual(Recipe.select().count(), 0)

        read_crop_schedule.return_value = self.mock_schedule()
        read_recipes.return_value = self.mock_recipes()
        Ingester('mock_plan')

        cultivars = Cultivar.select()
        self.assertEqual(len(cultivars), 3)
        self.assertListEqual([c.name for c in cultivars], ['basil', 'arugula', 'butterhead'])
        self.assertEqual(Recipe.select().count(), 2)

    @mock.patch.object(Ingester, 'read_crop_schedule')
    @mock.patch.object(Ingester, 'read_recipes')
    @mock.patch.object(Ingester, 'read_recipe_recommendations')
    def test_crop_schedule_and_recommendations_are_correctly_mapped(self, read_recipe_recommendations, read_recipes, read_crop_schedule):
        '''TODO: datetime formatting issue using mock in test. this test is to ensure that rows from the crop_schedule
        sheet are are joined to the correct row in the recommendation sheet'''
        pass

    @mock.patch.object(Ingester, 'read_crop_schedule')
    @mock.patch.object(Ingester, 'read_recipes')
    def lots_are_correctly_formatted(self):
        '''TODO: this test is to ensure that the lots dataframe maintains the correct formatting to send over a Celery payload:
            - dates: Celery and RabbitMQ do not support datetime objects. instead, convert to iso8601 format in keeping with teh JSON standard
            - recipe_recommendations: these are ingested as string and need to converted into 
            a list of integers for validation in the Lot model
        '''
        pass

    def lots_are_enqueued_correctly(self):
        '''TODO: this test is to ensure that each of the lots are processed through a separate Celery job. We would also want some additional
        tests to ensure that the Celery job is generating the correct RabbitMQ routing key so that the message is added to the correct queue
        '''
        pass


    '''
    TODO: can refactor these into functions that can dynamically generate different dataframes to test other cases
    this is especially useful in the case of validating edge cases for incoming data, missing DB records, etc. 
    '''

    def mock_recipes(self):
        return pd.DataFrame([
            {'id': 1, 'light_intensity': 1, 'lights_off': 0, 'lights_on': 10, 'co2_ppm': 400}, 
            {'id': 2, 'light_intensity': 2, 'lights_off': 0, 'lights_on': 20, 'co2_ppm': 300}
        ])

    def mock_schedule(self):
        return pd.DataFrame([
            {'date': datetime.date('2022-12-25 00:00:00'), 'cultivar_name': 'Basil', 'crop_count': 10, 'farm_id': 1, 'default_recipe': 1}, 
            {'date': datetime.date('2022-12-25 00:00:00'), 'cultivar_name': 'Arugula', 'crop_count': 10, 'farm_id': 1, 'default_recipe': 1},
            {'date': datetime.date('2022-12-25 00:00:00'), 'cultivar_name': 'Butterhead', 'crop_count': 10, 'farm_id': 1, 'default_recipe': 1}
        ])
        
    def mock_recommendations(self):
        return pd.DataFrame([
            {'cultivar_name': 'Basil', 'valid_for_date': '2022-04-06 00:00:00', 'recipe_ids': '1,2,3'}, 
            {'cultivar_name': 'Arugula', 'valid_for_date': '2022-04-06 00:00:00', 'recipe_ids': '2'}, 
            {'cultivar_name': 'Butterhead', 'valid_for_date': '2022-04-10 00:00:00', 'recipe_ids': '1'}, 
        ])