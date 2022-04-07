import peewee
from peewee import *
from datetime import datetime

proxy = peewee.Proxy()
db = peewee.SqliteDatabase('DEV_DB')
proxy.initialize(db)

class BaseModel(peewee.Model):
  class Meta:
    database = proxy

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

db.create_tables(models = [Farm, Recipe, Cultivar, Batch], safe=True)
Farm.insert(name='test_site').on_conflict_replace().execute()