from concurrent.futures import process
from celery import Celery
from publisher import Publisher
from schema import db, Cultivar, Recipe, Batch

app = Celery('tasks', broker='amqp://localhost')
publisher = Publisher()

# Ingester:
#     - reads in recommendations and crop_schedule 
#     - combines data into lot records

# -> for each lot record:
#     -> handoff lot to a worker to process


# Worker:
# -> for each batch in lot:
#   -> validate base level batch data 
#   -> (scheduled_date, valid_until_date should not have expired)
#   -> (cultivar should be supported)
#   -> validate the recipe recommendation

#   -> generate batch payload
#   -> enqueue to the correct task queue for the batch's scheduled_date

class LotHandler():
    
    @app.task
    def __init__(self, lot):
        self.store_cleaned(lot)

    def store_cleaned(self, lot):
        # TODO: raise for different attribute errors. 
        # validation: what if crop_count is 0 or 1? what about for the other fields (name, scheduled_date, valid_for_date, recommendations, default_recipe)?
        # see pandas converters for this as well. maybe a list of lambdas or small functions that enforce the incoming data

        # additional check that valid_for_date has not passed 
        self.name = lot['name']
        self.crop_count = int(lot['crop_count'])
        self.scheduled_date = lot['scheduled_date']
        self.valid_for_date = lot['valid_for_date']
        self.farm_id = lot['farm_id']
        self.default_recipe = int(lot['default_recipe'])
        self.recommendations = self.__backfill_recommendations(lot['recommendations'])

    def enqueue_batches(self):
        #TODO: add logger lines to allow batch tracking
        publisher = Publisher().declare_queue(f"batch_queue_{self.scheduled_date}")
        publisher.enqueue(self.batches())
    
    def batches(self):
        batches = []
        for i in range(0, self.lot['crop_count']):
            recipe_id = self.recommendations[i]
            if self.__is_valid_recipe(recipe_id):
                batch = {
                    'cultivar_name': self.name, 
                    'scheduled_date': self.scheduled_date,
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
                    batch_id = Batch.insert(batch).execute()
                except:
                    # TODO: retry 3 times in rollback fashion or skip to next iteration?
                    pass

                batch['id'] = batch_id
                batches.append(batches)

        return batches

    def __is_valid_recipe(self, recipe_id):
        # TODO: THIS APPROACH SEEMS BRITTLE: THE LOT HANDLER BE ABLE TO VALIDATE LOT DATA FROM OTHER SOURCES THAN INGESTER
        # note: at this point we should have ensured that recommendations and lots have not expired since:
            # (1) the left join should match the recommendation list for each crop with associated cultivar lot of the day
            # (2) the scheduled lot itself has not expired

        # TODO: bool(get_cached_recipe || database_call)
        return bool(Recipe.get_or_none(Recipe.id == recipe_id))

    def __backfill_recommendations(self, recommendations):
        ret = recommendations
        diff = self.crop_count - len(recommendations)
        if diff > 0:
            ret.append([self.default_recipe] * diff)

        return ret


# def generate_payloads(lot):
#     # validate_lot()
#     batches = []

#     for i in range(0, lot['crop_count']):
#         # recipe_id = valid_batch_recipe()
#         batch = {
#             'cultivar_name': name, 
#             'scheduled_date': scheduled_date,
#             'valid_for_date': valid_for_date,
#             'recipe_id': int(recipe_id), # can we coerce the int in the validations lambdas section
#             'farm_id': lot['farm_id']
#         }

#         batches.append(batch)

#     return batches


# def enqueue(batch, queue_name):
#     publisher.declare_queue(queue_name)
#     publisher.enqueue(batch)

# def is_valid(recipe_id):
#     try: 
#         recipe = Recipe.get(Recipe.id == recipe_id)
#     except:
#         return False


# def batches(lot):
#     name = lot['cultivar_name'].lower()
#     scheduled_date = lot['date'].date()
#     valid_for_date = lot['valid_for_date'].date()
#     recommendations = (lot['recipe_ids']).split(',')
            
#     # TODO: validation: what if crop_count is 0 or 1? what about for the other fields (name, scheduled_date, valid_for_date, recommendations, default_recipe)?
#     # see pandas converters for this as well. maybe a list of lambdas or small functions that enforce the incoming data
#     for i in range(0, lot['crop_count']):
#         try:

#             recipe_id = recommendations[i]
#         except IndexError:
#             recipe_id = lot['default_recipe']


#         #TODO: metaprogramming to only pull the requisite fields?
#         batch = {
#             'cultivar_name': name, 
#             'scheduled_date': scheduled_date,
#             'valid_for_date': valid_for_date,
#             'recipe_id': int(recipe_id), # can we coerce the int in the validations lambdas section
#             'farm_id': lot['farm_id']
#         }
#         batches.append(batch)