#TODO validate all incoming data across the 3 spreadsheets! normalize as needed

import pandas as pd
from datetime import datetime
from lib.db import Cultivar, Recipe
from lib.tasks import enqueue_batches

'''note: if needed, multiple Ingester instances can be invoked through Celery jobs to process multiple 
spreadsheets in parallel. in that case, all IO operations including DB gets/upserts would also be performed 
asynchronously
'''

class Ingester():
    def __init__(self, plan):
        #TODO: can add additional validation to ensure spreadsheet submission
        self.crop_plan = plan
        self.crop_schedule = self.read_crop_schedule()
        self.upsert_cultivars()
        self.upsert_recipes()

    def upsert_cultivars(self):
        time = datetime.now()
        names = self.crop_schedule.loc[:, 'cultivar_name'].unique().tolist()
        cultivars = [{'name': n.lower(), 'created_at': time} for n in names]
        #TODO: make atomic
        Cultivar.replace_many(cultivars, fields=[Cultivar.name, Cultivar.created_at]).execute()
    
    def upsert_recipes(self):
        recipes = self.read_recipes()
        # validate incoming recipes
        #TODO: make atomic
        Recipe.replace_many(recipes.to_dict(orient='records')).execute()


    #TODO: refactor: one method that uses metaprogramming?
    def read_recipes(self):
        return pd.read_excel(self.crop_plan, sheet_name='recipes')

    def read_crop_schedule(self):
        return pd.read_excel(
            self.crop_plan, 
            sheet_name='crop schedule', 
            converters={'crop_count': int, 'farm_id': int, 'default_recipe': int}
        )
    
    def read_recipe_recommendations(self):
        return pd.read_excel(self.crop_plan, sheet_name='recipe_recommendations')

    def lots_data(self):
        lots = pd.merge(
            self.crop_schedule, 
            self.read_recipe_recommendations(),
            left_on=['cultivar_name', 'date'], 
            right_on=['cultivar_name', 'valid_for_date'],
            how='left'
        )

        lots['date'] = lots['date'].map(lambda x: x.isoformat())
        lots['valid_for_date'] = lots['valid_for_date'].map(lambda x: x.isoformat())
        lots['recipe_ids'] = lots['recipe_ids'].map(lambda x: [int(r) for r in x.split(',')])

        return lots

    def process_lots_data(self):
        for lot_data in self.lots_data().iterrows():
            enqueue_batches.delay(lot_data[1].to_json())
        
# ingester = Ingester('./crop_plan.xlsx').process_lots_data()
