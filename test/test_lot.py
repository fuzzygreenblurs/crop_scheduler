from unittest import mock
from test.test_base import TestBase
from lib.lot import Lot
from lib.db import *
from dateutil import parser
import pandas as pd
import logging
import pdb

class TestLot(TestBase):
    def setUp(self):
        super().setUp()
        Recipe.replace_many(self.mock_recipes().to_dict(orient='records')).execute()
        [Cultivar.create(name=c) for c in ['basil', 'arugula']]

    def test_will_use_recommended_recipes_first(self):
        mock_lot_data = self.mock_lot_data()
        mock_lot_data['crop_count'] = '2.3' # quick test for basic Lot level validation
        mock_lot_data['recipe_ids'] = list(range(1,5)) # recommendations

        payloads = Lot(mock_lot_data).batch_payloads
        assigned_batch_recipes = [p['recipe_id'] for p in payloads]
        self.assertEqual(assigned_batch_recipes, [1,2])

    def test_skip_invalid_recommendation_and_continue(self):
        # note: i assume a batch with an invalid recipe recommendation should be skipped 
        # and the next batch in the lot should be processed as expected. 
        # this behavior could also be updated so that the batch gets assigned the next recipe_id in queue

        mock_lot_data = self.mock_lot_data()
        mock_lot_data['crop_count'] = '4'
        mock_lot_data['recipe_ids'] = [0,1,2]

        payloads = Lot(mock_lot_data).batch_payloads
        assigned_batch_recipes = [p['recipe_id'] for p in payloads]
        self.assertEqual(assigned_batch_recipes, [1,2,1])

    def test_insufficient_recommendations_are_backfilled_with_default(self):
        # note: i assume a batch with an invalid recipe recommendation should be skipped 
        # and the next batch in the lot should be processed as expected. 
        # this behavior could also be updated so that the batch gets assigned the next recipe_id in queue

        mock_lot_data = self.mock_lot_data()
        mock_lot_data['crop_count'] = '5'
        mock_lot_data['recipe_ids'] = [1,2]

        payloads = Lot(mock_lot_data).batch_payloads
        assigned_batch_recipes = [p['recipe_id'] for p in payloads]
        self.assertEqual(assigned_batch_recipes, [1,2,1,1,1])

    def mock_lot_data(self):
        return {
            'date': '2022-12-25 00:00:00',
            'cultivar_name': 'Basil',
            'farm_id': 1,
            'default_recipe': '1',
            'valid_for_date': '2022-12-25 00:00:00'
        }

    def mock_recipes(self):
        return pd.DataFrame([
            {'light_intensity': 1, 'lights_off': 0, 'lights_on': 10, 'co2_ppm': 400}, 
            {'light_intensity': 6, 'lights_off': 0, 'lights_on': 100, 'co2_ppm': 300},
            {'light_intensity': 3, 'lights_off': 1, 'lights_on': 225, 'co2_ppm': 1000}
        ])