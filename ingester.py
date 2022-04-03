#TODO validate all incoming data across the 3 spreadsheets! normalize as needed

import pandas as pd
from datetime import datetime
from schema import db, Cultivar, Recipe, Batch
import pdb

# ingester code

class Ingester():
    def __init__(self, plan):
        self.crop_plan = plan
        self.crop_schedule = self.read_crop_schedule()
        self.upsert_cultivars()
        self.upsert_recipes()
        
        #TODO wait until upsert is completed and perform callback
        self.batches = self.generate_batches()

    def upsert_cultivars(self):
        time = datetime.now()
        names = self.crop_schedule.loc[:, "cultivar_name"].unique().tolist()
        cultivars = [{'name': n.lower(), 'created_at': time} for n in names]
        with db.atomic():
            Cultivar.replace_many(cultivars, fields=[Cultivar.name, Cultivar.created_at]).execute()
    
    def upsert_recipes(self):
        recipes = self.read_recipes()
        # validate incoming recipes
        with db.atomic():
            Recipe.replace_many(recipes.to_dict(orient='records')).execute()


    #TODO: refactor: one method that uses metaprogramming?
    def read_recipes(self):
        return pd.read_excel(self.crop_plan, sheet_name="recipes")

    def read_crop_schedule(self):
        return pd.read_excel(self.crop_plan, sheet_name="crop schedule", converters={'crop_count': int, 'farm_id': int, 'default_recipe': int})
    
    def read_recipe_recommendations(self):
        return pd.read_excel(self.crop_plan, sheet_name="recipe_recommendations")

    #TODO: refactor: break this up into smaller methods as needed!
    def generate_batches(self):
        batches = []

        schedule = self.read_crop_schedule()
        recommendations = self.read_recipe_recommendations()
        
        #TODO: ensure that recommendation list is not expired (how to handle older dates?)
        lots = pd.merge(
            schedule, 
            recommendations,
            left_on=['cultivar_name', 'date'], 
            right_on=['cultivar_name', 'valid_for_date'],
            how='left'
        )
        
        #TODO: spawn async job for each lot to perfom this computation and enqueue for future processing (each day has its own task queue)
        # hit cache to ensure recipes exist, else get from DB, else use default_id (validate that default_recipe is included, else use hardcoded value)
        # note: we want to spawn jobs to asynchronously perform all the cache hits/DB checks to validate the recipes as needed!
        for lot in lots.iterrows():
            lot = lot[1] # this reassignment is just to accommdate pandas dataframe format
            
            name = lot['cultivar_name'].lower()
            scheduled_date = lot['date'].date()
            valid_for_date = lot['valid_for_date'].date()
            recommendations = (lot['recipe_ids']).split(',')
            
            # TODO: validation: what if crop_count is 0 or 1? what about for the other fields (name, scheduled_date, valid_for_date, recommendations, default_recipe)?
            # see pandas converters for this as well. maybe a list of lambdas or small functions that enforce the incoming data
            for i in range(0, lot['crop_count']):
                try:
                    recipe_id = recommendations[i]
                except IndexError:
                    recipe_id = lot['default_recipe']


                #TODO: metaprogramming to only pull the requisite fields?
                batch = {
                    'cultivar_name': name, 
                    'scheduled_date': scheduled_date,
                    'valid_for_date': valid_for_date,
                    'recipe_id': int(recipe_id), # can we coerce the int in the validations lambdas section
                    'farm_id': lot['farm_id']
                }
                batches.append(batch)

        return batches

i = Ingester("./crop_plan.xlsx")

for batch in i.batches:
    print(batch)