#TODO validate all incoming data across the 3 spreadsheets! normalize as needed

import pandas as pd
from datetime import datetime
from schema import db, Cultivar, Recipe
from tasks import enqueue_tasks, test
import pdb


'''note: if needed, multiple Ingester instances can be invoked through Celery jobs to process multiple 
spreadsheets in parallel. in that case, all IO operations (including upsert_cultivars & upsert_recipes)
would also be performed asynchronously
'''
class Ingester():
    def __init__(self, plan):
        self.crop_plan = plan
        self.crop_schedule = self.read_crop_schedule()
        self.upsert_cultivars()
        self.upsert_recipes()

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
        return pd.read_excel(
            self.crop_plan, 
            sheet_name="crop schedule", 
            converters={'crop_count': int, 'farm_id': int, 'default_recipe': int}
        )
    
    def read_recipe_recommendations(self):
        return pd.read_excel(self.crop_plan, sheet_name="recipe_recommendations")

    def lots(self):
        lots = pd.merge(
            self.read_crop_schedule(), 
            self.read_recipe_recommendations(),
            left_on=['cultivar_name', 'date'], 
            right_on=['cultivar_name', 'valid_for_date'],
            how='left'
        )
        lots['date'] = lots['date'].map(lambda x: x.isoformat())
        lots['valid_for_date'] = lots['valid_for_date'].map(lambda x: x.isoformat())
        lots['recipe_ids'] = lots['recipe_ids'].map(lambda x: [int(r) for r in x.split(',')])

        return lots

    def trigger_batch_tasks(self):
        for lot in self.lots().iterrows():
            enqueue_tasks(lot[1].to_json())
        
Ingester("./crop_plan.xlsx").trigger_batch_tasks()
