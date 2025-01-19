from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger, perf_log


def analytics_9(spark: pyspark.sql.SparkSession, config : dict) -> None :
    """
    
    Description:
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None
    
    Notes:

        either VEH_DMAG_SCL_1_ID or VEH_DMAG_SCL_2_ID should be above 4
        FIN_RESP_TYPE_ID should contain 'INSURANCE'


    """

    units_df = read_file(spark, config, 'units')
    damages_df = read_file(spark, config, 'damages')


    car_crashes_df = units_df.drop_duplicates(['CRASH_ID', 'UNIT_NBR']).filter(SF.upper(SF.col('VEH_BODY_STYL_ID')).rlike('.*CAR.*'))

    car_crashes_df_1 = car_crashes_df.withColumn('VEH_DMAG_SCL_1_ID', SF.regexp_substr(SF.col('VEH_DMAG_SCL_1_ID'), SF.lit(r'\d+')).cast(IntegerType()))
    car_crashes_df_2 = car_crashes_df_1.withColumn('VEH_DMAG_SCL_2_ID', SF.regexp_substr(SF.col('VEH_DMAG_SCL_2_ID'), SF.lit(r'\d+')).cast(IntegerType()))

    crashes_with_insurance_df = car_crashes_df_2.filter((SF.col('VEH_DMAG_SCL_1_ID') > 4) | (SF.col('VEH_DMAG_SCL_2_ID') > 4)).filter(SF.upper(SF.col('FIN_RESP_TYPE_ID')).rlike('.*INSURANCE.*'))

    crashes_with_no_property_damage_df = crashes_with_insurance_df.join(damages_df, on=['CRASH_ID'], how='left_anti')

    result_df =  crashes_with_no_property_damage_df.select('CRASH_ID').distinct().cache()

    write_df(spark, result_df, config, 'analytics_9')

    result = result_df.count()

    logger.info(f'Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance = {result}')

