from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger, perf_log



def analytics_4(spark: pyspark.sql.SparkSession, config : dict) -> None:
    """

    Description:
        Determines the number of Vehicles with driver having valid licences involved in hit and run

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None
    
    Notes:
        doesn't consider the scenario where the same vehicle has been involved in multiple hit and runs

    """

    charges_df = read_file(spark, config, 'charges')
    persons_df = read_file(spark, config, 'primary_person')

    # considering the charges "Leaving from scene/accident" to be hit and run
    charges_df_1 = charges_df.filter(SF.upper(SF.col('CHARGE')).rlike('.*LEAVING.*(SCENE|ACCIDENT).*')).select('CRASH_ID', 'UNIT_NBR', 'PRSN_NBR').distinct()
    persons_df_1 = persons_df.filter((SF.upper(SF.col('PRSN_TYPE_ID')).rlike('.*DRIVER.*')) & ~(SF.trim(SF.upper(SF.col('DRVR_LIC_TYPE_ID'))).isin('', 'NA', 'UNKNOWN'))).select('CRASH_ID', 'UNIT_NBR', 'PRSN_NBR').distinct()

    hit_and_runs = charges_df_1.join(persons_df_1, on=['CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'], how='inner')

    result_df = hit_and_runs.select('CRASH_ID', 'UNIT_NBR').distinct().cache()

    write_df(spark, result_df, config, 'analytics_4')

    result = result_df.count()
    logger.info(f'Number of Vehicles with driver having valid licences involved in hit and run = {result}')

    return result
