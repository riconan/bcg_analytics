from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger


def analytics_2(spark: pyspark.sql.SparkSession, config : dict) -> None :
    """
    Description:
        Two wheelers booked for crashes

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
    charges_df = read_file(spark, config, 'charges')

    units_df_1 = units_df.select('CRASH_ID', 'UNIT_NBR').filter(SF.col('VEH_BODY_STYL_ID').isin('POLICE MOTORCYCLE', 'MOTORCYCLE')).distinct()
    charges_df_1 = charges_df.select('CRASH_ID', 'UNIT_NBR').distinct()

    charged_motorcycles = units_df_1.join(charges_df_1, on=['CRASH_ID', 'UNIT_NBR'], how='inner')

    result_df = charged_motorcycles

    write_df(spark, result_df, config, 'analytics_2')
    
    result = result_df.count()
    logger.info(f'Total Two wheelers booked for crashes is = {result}')