from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger


def analytics_3(spark: pyspark.sql.SparkSession, config : dict) -> pyspark.sql.DataFrame :
    """

    Description:
        Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None
    
    Notes:
        'NA' VEH_BODY_STYL_ID is not considered

    """

    persons_df = read_file(spark, config, 'primary_person')
    units_df = read_file(spark, config, 'units')

    persons_df_1 = persons_df.filter((SF.col('PRSN_TYPE_ID') == 'DRIVER') & (SF.col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED') & (SF.col('DEATH_CNT') == 1)).select('CRASH_ID', 'UNIT_NBR').distinct()
    
    cars_df = units_df.filter(SF.upper(SF.col('VEH_BODY_STYL_ID')).rlike('.*CAR.*'))

    cars_df_1 = cars_df.select('CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID').filter((SF.col('VEH_MAKE_ID') != 'NA') & (SF.col('DEATH_CNT') > 0)).distinct()

    vehicle_makes = cars_df_1.join(persons_df_1, on=['CRASH_ID', 'UNIT_NBR'], how='inner')

    vehicle_makes_occurrences = vehicle_makes.groupBy('VEH_MAKE_ID').agg(SF.count("*").alias('occurrences'))

    result_df = vehicle_makes_occurrences.orderBy(SF.col('occurrences').desc()).limit(5).cache()

    write_df(spark, result_df, config, 'analytics_3')
    
    result = [(str(i.VEH_MAKE_ID), str(i.occurrences)) for i in result_df.collect()]
    logger.info(f'Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy = {result}')

    return result