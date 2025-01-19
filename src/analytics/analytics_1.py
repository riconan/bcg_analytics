from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger, perf_log


def analytics_1(spark: pyspark.sql.SparkSession, config : dict) -> None :

    """
    Description:
        Finds the number of crashes (accidents) in which number of males killed are greater than 2

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None

    """
    
    primary_person_df = read_file(spark, config, 'primary_person')

    primary_person_df_1 = primary_person_df.select('CRASH_ID', 'UNIT_NBR', 'PRSN_NBR', 'PRSN_GNDR_ID', 'DEATH_CNT')

    # necesary records - the person should be male and he should be dead
    primary_person_df_2 = primary_person_df_1.filter((SF.col('PRSN_GNDR_ID') == SF.lit('MALE')) & (SF.col('DEATH_CNT') == SF.lit('1')))

    accident_group = primary_person_df_2.groupBy('CRASH_ID')

    people_involved_df = accident_group.agg(SF.count_distinct('UNIT_NBR', 'PRSN_NBR').alias('people_involved'))

    people_involved_df_1 = people_involved_df.filter(SF.col('people_involved') > SF.lit(2))

    # since we process only the records of interest (male and death only). the number people involved is the result
    result_df = people_involved_df_1

    write_df(spark, result_df, config, 'analytics_1')

    result = result_df.count()
    
    logger.info(f'Total Accidents where the number of male deaths is > 2 = {result}')
