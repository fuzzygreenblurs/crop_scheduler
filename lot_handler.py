from schema import db, Cultivar, Recipe, Batch, Farm
from dateutil import parser
import pdb

class LotHandler():
    def __init__(self, lot):
        self.store_clean(lot)
        self.batch_payloads = self.generate_batch_payloads()

    def store_clean(self, lot):
        # TODO: validation: raise for different attribute errors. 
            # what if crop_count is 0 or 1? 
            # what about for the other fields (name, scheduled_date, valid_for_date, recommendations, default_recipe)?
            # cultivar should be supported as well
        # see pandas converters for this as well. maybe a list of lambdas or small functions that enforce the incoming data
        # additional check that valid_for_date has not passed 

        #TODO: metaprogramming to only pull the requisite fields?
        self.date = parser.parse(lot['date'])
        self.cultivar_name = lot['cultivar_name'].lower()
        self.crop_count = int(lot['crop_count'])
        self.farm_id = lot['farm_id']
        self.default_recipe = int(lot['default_recipe'])
        self.valid_for_date = parser.parse(lot['valid_for_date'])
        self.recipe_ids = self.__backfill_recommendations(lot['recipe_ids'])
    
    def generate_batch_payloads(self):
        #TODO: refactor this method
        payloads = []

        for i in range(self.crop_count):
            recipe_id = self.recipe_ids[i]
            if self.__is_valid_recipe(recipe_id):
                payload = {
                    'cultivar_id': Cultivar.get(Cultivar.name == self.cultivar_name).id, #TODO: move to section and retain id as instance attr for batch creation
                    'scheduled_date': self.date,
                    'valid_for_date': self.valid_for_date,
                    'recipe_id': recipe_id, # can we coerce the int in the validations lambdas section
                    'farm_id': self.farm_id
                }

                ''' 
                note: sqlite does not support the RETURNING clause, so we cannot perform a 
                bulk insert. for this demo, im simply retrieving the requisite batch_id for each batch
                as a loop of single DB calls, although I would chunk this in production
                '''

                try:
                    id = Batch.insert(payload).execute()
                except:
                    # TODO: retry 3 times in rollback fashion or skip to next iteration?
                    continue

                payload['id'] = id
                payloads.append(payload)

        return payloads

    # def __get_cultivar(self, name):
    #     return Cultivar.get_or_none(Cultivar.name == name)

    # def __get_farm(self, id):
    #     return Farm.get_or_none(Farm.id == id)

    # def __get_recipe(self, recipe_id):
    #     return Recipe.get_or_none(Recipe.id == recipe_id)

    def __is_valid_recipe(self, recipe_id):
        # TODO: this approach can be brittle: the lot handler must be able to validate lot data from other sources if needed
        '''note: at this point we should have ensured that recommendations and lots have not expired since:
            # (1) the left join should match the recommendation list for each crop with associated cultivar lot of the day
            # (2) the scheduled lot itself has not expired
        '''

        # TODO: bool(get_cached_recipe || database_call)
        return bool(Recipe.get_or_none(Recipe.id == recipe_id))

    def __backfill_recommendations(self, recipe_ids):
        ret = recipe_ids
        backfill = self.crop_count - len(recipe_ids)
        if backfill > 0:
            ret.extend([self.default_recipe] * backfill)

        return ret