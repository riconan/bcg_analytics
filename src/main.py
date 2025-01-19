# %%
"""
This script enables us to execute the analytics

Usage:
    we can do spark-submit with this file

Command Line Arguments:
    analytics_choice : 1-10 or 'all' ('all' will execute all the analytics sequentially)

"""

# %%
from utils.logging import logger, perf_log

from analytics import get_analytics_function

import json


# %%

config = None

with open('config.json') as file:

    config = json.load(file)

# %%
from pyspark.sql import SparkSession

spark = SparkSession.Builder().appName('bcg-analytics').getOrCreate()

# %%
import argparse

parser = argparse.ArgumentParser()

# required positional argument

parser.add_argument('analytics_choice', type=str, help="provide the choice for analytics : an integer or 'all' ")

args = parser.parse_args()

choice = str(args.analytics_choice).strip()


if(choice == 'all'):

    for i in range(1, 11):

        analytics_id = f'analytics_{i}'

        func = perf_log(get_analytics_function(f'analytics_{i}'))

        func(spark, config)


elif(int(choice)):

    analytics_id = f'analytics_{choice}'

    func = perf_log(get_analytics_function(analytics_id))

    func(spark, config)

else:
    raise ValueError(f"expected values are integer (1-10) / 'all'")



