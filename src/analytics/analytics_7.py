from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger, perf_log



def analytics_7(spark: pyspark.sql.SparkSession, config : dict) -> None:
    """
    Description:
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None        
    """

    units_df = read_file(spark, config, 'units')
    units_df_1 = units_df.select('CRASH_ID', 'UNIT_NBR', 'VEH_BODY_STYL_ID').drop_duplicates(['CRASH_ID', 'UNIT_NBR'])

    persons_df = read_file(spark, config, 'primary_person')
    persons_df_1 = persons_df.select('CRASH_ID', 'UNIT_NBR', 'PRSN_NBR', 'PRSN_ETHNICITY_ID').drop_duplicates(['CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'])

    body_style_and_ethnicity = units_df_1.join(persons_df_1, on=['CRASH_ID', 'UNIT_NBR'], how='inner').groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count()

    window = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(SF.col('count').desc())
    body_style_and_ethnicity_ranked = body_style_and_ethnicity.withColumn('rank', SF.row_number().over(window))

    result_df = body_style_and_ethnicity_ranked.filter(SF.col('rank') == 1).select('VEH_BODY_STYL_ID', SF.col('PRSN_ETHNICITY_ID').alias('top_ethnic_group')).cache()

    write_df(spark, result_df, config, 'analytics_7')

    result = [(str(i.VEH_BODY_STYL_ID), str(i.top_ethnic_group)) for i in result_df.collect()]

    logger.info(f'For all the body styles involved in crashes, mention the top ethnic user group of each unique body style = {result}')
