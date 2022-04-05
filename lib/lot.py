from lib.db import Cultivar, Recipe, Batch
from dateutil import parser
import logging
import pdb

class Lot():
    def __init__(self, lot):
        self.store_clean(lot)
        self.batch_payloads = self.generate_batch_payloads()

    def store_clean(self, lot):
        '''
        TODO: validation: raise for different attribute errors: 
            - what if crop_count is 0 or 1? 
            - what about for the other fields (name, scheduled_date, valid_for_date, recommendations, default_recipe)?
            - only supported cultivars and farms should be accepted!
            - see pandas converters for this as well: maybe a list of lambdas or small functions that enforce the incoming data
            - additional check that the scheduled batch date or the recommendation's valid_for_date has not elapsed yet
        '''

        #TODO: metaprogramming with validated data to only pull the requisite fields:
        # this will make this method much simpler to read!
        self.date = parser.parse(lot['date'])
        self.cultivar_name = Cultivar.get(Cultivar.name == lot['cultivar_name'].lower()).name
        self.crop_count = max(int(float(lot['crop_count'])), 1)
        self.farm_id = lot['farm_id']
        self.default_recipe = max(int(float(lot['default_recipe'])), 1)
        self.valid_for_date = parser.parse(lot['valid_for_date'])
        self.recipe_ids = self.__backfill_recommendations(lot['recipe_ids'])
    
    def generate_batch_payloads(self):
        #TODO: refactor this method into smaller chunks for readability
        payloads = []

        for i in range(self.crop_count):
            recipe_id = self.recipe_ids[i]
            if self.__is_valid_recipe(recipe_id):
                payload = {
                    'cultivar_id': Cultivar.get(Cultivar.name == self.cultivar_name).id,
                    'scheduled_date': self.date,
                    'valid_for_date': self.valid_for_date,
                    'recipe_id': recipe_id,
                    'farm_id': self.farm_id
                }

                ''' 
                note: sqlite does not support the RETURNING clause (required to retrieve the batch_id), so I am unable 
                perform a bulk insert in this case. for this demo, im simply retrieving the requisite batch_id 
                for each batch as a loop of single DB calls, although I would chunk this in a production setting
                '''
                try:
                    id = Batch.insert(payload).execute()
                except:
                    # TODO: retry 3 times in rollback fashion or skip to next iteration
                    continue

                payload['id'] = id
                payloads.append(payload)

        return payloads

    def __is_valid_recipe(self, recipe_id):
        '''
        TODO: this approach can be brittle: the lot handler must be able to validate lot data from other sources if needed
        note: at this point we should have ensured that recommendations and lots have not expired since:
            # (1) the left join should match the recommendation list for each crop with associated cultivar lot of the day
            # (2) the scheduled lot itself has not expired
        '''

        '''
        # TODO: bool(get_cached_recipe or database_call):
            in order to avoid more expensive database_calls, first hit a cache layer. the cache can potentially use a write-through
            architecture (slower but simpler architecture) to maintain parity with the database. note that a write-behind would 
            be more time efficient but would require additional redundancy at the cache layer since it is in-memory and is 
            therefore susceptible to power loss, network failure, etc.this type of failure at the cache layer would result in 
            large chunks of missing or out of date batch records
        '''
        return bool(Recipe.get_or_none(Recipe.id == recipe_id))

    def __backfill_recommendations(self, recipe_ids):
        ret = recipe_ids
        backfill = int(self.crop_count) - len(recipe_ids)
        if backfill > 0:
            ret.extend([self.default_recipe] * backfill)

        return ret
