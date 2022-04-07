from pydantic import BaseModel, ValidationError, root_validator, validator
from lib.db import Cultivar, Recipe, Batch, Farm
from datetime import datetime
from dateutil import parser
import logging
import pdb

class Lot(BaseModel):
    date: datetime
    cultivar_name: str
    crop_count: int
    farm_id: int
    default_recipe: int
    valid_for_date: datetime
    recipe_ids: list[int] = []

    @root_validator
    def validate_referenced_fields(cls, values):
        values['cultivar_name'] = values['cultivar_name'].lower()

        if not Cultivar.get_or_none(Cultivar.name == values['cultivar_name']): raise ValidationError(f"unsupported Cultivar: {values['cultivar_name']}")
        if not Farm.get_or_none(Farm.id == values['farm_id']): raise ValidationError(f"unsupported Farm: {values['farm_id']}")
        if not Recipe.get_or_none(Recipe.id == values['default_recipe']): raise ValidationError(f"unsupported Recipe: {values['default_recipe']}")

        return values

    @root_validator
    def validate_timestamps_have_not_elapsed(cls, values):
        current_time = datetime.now()
        if current_time >= values['date'] or current_time >= values['valid_for_date']:
            raise ValidationError(f"Lot deadlines have elapsed.")

        return values
    
    @validator('crop_count')
    def validate_batch_count_is_positive(cls, value):
        if value < 1: raise ValueError(f"Lot batch count must be atleast 1.")
        return value

    #note: if batch_payloads ends up being used repeatedly, this method can be memoized 
    def batch_payloads(self):
        self.__backfill_recommendations()
        
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
                    # TODO: can retry in some kind of rollback fashion or skip to next iteration
                    continue

                payload['id'] = id
                payloads.append(payload)

        return payloads

    def __is_valid_recipe(self, recipe_id):
        '''
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

    def __backfill_recommendations(self):
        # TODO: could make this more idiomatic
        ret = self.recipe_ids.copy()
        backfill = self.crop_count - len(self.recipe_ids)
        if backfill > 0:
            ret.extend([self.default_recipe] * backfill)

        self.recipe_ids = ret