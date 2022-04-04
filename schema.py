#TODO only import requisite classes from peewee
#TODO do we need relationships between the tables?

from peewee import *
from datetime import datetime 
# import pdb

db = SqliteDatabase('farm_ops.db')
class BaseModel(Model):
    class Meta:
        database = db
        
class Farm(BaseModel):
    name = CharField(unique=True)
    created_at = TimestampField(default=datetime.now())
    updated_at = TimestampField(null=True)

class Cultivar(BaseModel):
    name = CharField(unique=True)
    created_at = TimestampField(default=datetime.now())
    updated_at = TimestampField(null=True)

class Recipe(BaseModel):
    light_intensity = FloatField()
    lights_off = FloatField()
    lights_on = FloatField()
    co2_ppm = FloatField()
    created_at = TimestampField(default=datetime.now())
    updated_at = TimestampField(null=True)

class Batch(BaseModel):
    farm_id = ForeignKeyField(Cultivar)
    cultivar_id = ForeignKeyField(Cultivar)
    recipe_id = ForeignKeyField(Recipe, default=1)
    in_progress = BooleanField(default=False)
    scheduled_date = TimestampField(null=True)
    valid_for_date = TimestampField(null=True)
    created_at = TimestampField(default=datetime.now())
    updated_at = TimestampField(null=True)

db.connect()
models = [Farm, Cultivar, Recipe, Batch]

# setup tables
#TODO: move this into a set of migrations and run from Invoke script
db.drop_tables(models)
db.create_tables(models)

Farm.create(name="brooklyn_site")

