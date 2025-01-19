from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger, perf_log



def analytics_8(spark: pyspark.sql.SparkSession, config : dict) -> None :
    
    """
    Description:
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None
    
    Notes:
        only drivers with PRSN_ALC_RSLT_ID as 'POSITIVE' is considered
        And VEH_BODY_STYL_ID should contain 'CAR'

    """

    units_df = read_file(spark, config, 'units')
    units_df_1 = units_df.select('CRASH_ID', 'UNIT_NBR', 'VEH_BODY_STYL_ID').filter(SF.upper(SF.col('VEH_BODY_STYL_ID')).rlike('.*CAR.*')).drop_duplicates(['CRASH_ID', 'UNIT_NBR'])

    persons_df = read_file(spark, config, 'primary_person')
    persons_df_1 = persons_df.select('CRASH_ID', 'UNIT_NBR', 'PRSN_NBR', 'PRSN_TYPE_ID', 'PRSN_ALC_RSLT_ID', 'DRVR_ZIP')
    persons_df_2 = persons_df_1.filter((SF.upper(SF.col('PRSN_ALC_RSLT_ID')).rlike('.*POSITIVE.*')) & (SF.trim(SF.upper(SF.col('PRSN_TYPE_ID'))) == 'DRIVER'))
    persons_df_3 = persons_df_2.drop_duplicates(['CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'])


    dwi_car_crashes_df = units_df_1.join(persons_df_3, on=['CRASH_ID', 'UNIT_NBR'], how='inner').groupBy('DRVR_ZIP').agg(SF.count('*').alias('count'))
    dwi_car_crashes_ordered_df = dwi_car_crashes_df.orderBy(SF.col('count').desc()).filter(SF.col('DRVR_ZIP') != 'NA')

    result_df = dwi_car_crashes_ordered_df.limit(5).cache()

    write_df(spark, result_df, config, 'analytics_8')

    result = [(str(i['DRVR_ZIP']), str(i['count'])) for i in result_df.collect()]
    logger.info(f'Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash = {result}')
