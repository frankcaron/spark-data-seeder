# ==========================
# = Spark Data Seed Script
# = 
# = Frank Caron
# = frankcaron.com
# =
# = Generates a RDD in Spark with the desired 
# = mock data for test applications to consume.
# =
# = August 2015
# = 
# ==========================

# ------ Imports -------
from pyspark.sql import *
from pyspark.sql.types import *

import time
import random
import decimal
from datetime import datetime

from random import randint
from random import randrange
from datetime import timedelta

# Outdated 
# from pyspark.mllib.random import RandomRDDs

# ------ Helper -------
def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

# ------ Config -------
num_rows = 1000                                                     #number of rows to generate
table_name = 'automated_data_good'                                  #your table name
d1 = datetime.strptime('1/1/2014 1:30 PM', '%m/%d/%Y %I:%M %p')     #start date
d2 = datetime.strptime('1/1/2016 4:50 AM', '%m/%d/%Y %I:%M %p')     #end date

# Specify any specific enums needed for the data (e.g., for specific strings)
channels = ["social media", "billboard", "tv", "flyer", "radio"]
devices = ["mobile", "desktop", "tablet", "kiosk", "tv", "none"]
dates = []
sales = []

# Populate additional arrays with generated data (e.g., for sales numbers or dates)
for x in range(0, num_rows):
  rand_time_multiplier = randint(0, 1000000)
  dates.append(random_date(d1, d2))
  sales.append(round(random.uniform(0, 100), 2))

# Map the mock data to the rows
rando = sc.parallelize(dates).map(lambda date : (date, 
                                                 channels[randint(0,len(channels)-1)], 
                                                 devices[randint(0,len(devices)-1)], 
                                                 sales[randint(0,len(sales)-1)], 
                                                 random.uniform(66.885444,124.848974), 
                                                 random.uniform(24.396308,49.384358)))

# Register the DataFrames as a temp table.
salesDataFrame = sqlContext.createDataFrame(rando)#, schema)
salesDataFrame.registerTempTable("automated_data_temp_table")

# ------ Data Validation -------

# Preview registered temp table
# salesDataFrame.printSchema()
# results = sqlContext.sql("SELECT * FROM frank_table")
# for p in results.collect(): print p
  
# ------ Data Cleanup -------

# Preview registered temp table
results = sqlContext.sql("SELECT `_1` as date, `_2` as channel, `_3` as device, `_4` as amount, `_5` as lat, `_6` as lon FROM automated_data_temp_table")
for p in results.collect(): print p

# ------ Data Persistence -------
# Persist the legitimate table, overwriting any existing data 
# salesDataFrame.saveAsTable(table_name, mode="overwrite")
results.registerTempTable(table_name)
