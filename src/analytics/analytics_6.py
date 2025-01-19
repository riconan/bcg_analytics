from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger, perf_log


def analytics_6(spark: pyspark.sql.SparkSession, config : dict) -> None:
    """
    Description:
        Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None
    
    Notes:
        comparison is done by summing up injuries and death for each vehicle make
        
    """

    units_df = read_file(spark, config, 'units')
    units_df_1 = units_df.select('CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID', 'TOT_INJRY_CNT', 'DEATH_CNT').drop_duplicates(['CRASH_ID', 'UNIT_NBR'])

    # considering a death as an injury
    total_injury_df = units_df_1.groupBy('VEH_MAKE_ID').agg(SF.sum(SF.col('TOT_INJRY_CNT').cast(IntegerType()) + SF.col('DEATH_CNT').cast(IntegerType())).alias('total_injury'))

    # entire dataset is in a single window
    window = Window.partitionBy(SF.lit(1)).orderBy(SF.col('total_injury').desc())
    total_injury_ranked_df = total_injury_df.withColumn('rank', SF.row_number().over(window)).orderBy(SF.col('rank'))

    result_df = total_injury_ranked_df.filter((SF.col('rank') >= 3) & (SF.col('rank') <= 5)).drop('rank').cache()

    write_df(spark, result_df, config, 'analytics_6')

    result = [(str(i.VEH_MAKE_ID), str(i.total_injury)) for i in result_df.collect()]
    logger.info(f'State having the highest number of accidents in which females are not involved = {result}')
