from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger, perf_log


def analytics_5(spark: pyspark.sql.SparkSession, config : dict) -> None:
    """

    Description:
        Which state has highest number of accidents in which females are not involved

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None
    
    Notes:
        considering License Plate State of vehicle instead of driver license state as people are easier to migrate to other state.
        unknown genders and states are not considered for simplification

    """

    persons_df = read_file(spark, config, 'primary_person')
    units_df = read_file(spark, config, 'units')

    units_df_1 = units_df.select('CRASH_ID', 'VEH_LIC_STATE_ID').distinct()
    persons_df_1 = persons_df.select('CRASH_ID', 'PRSN_GNDR_ID').distinct()

    # marking all non-males as females; this will ensure that unkown genders are not considered for calcultaion
    persons_df_2 = persons_df_1.withColumn('is_female', SF.upper(SF.col('PRSN_GNDR_ID')) != 'MALE')
    units_df_2 = units_df_1.filter(SF.col('VEH_LIC_STATE_ID') != 'NA')

    accidents_without_females_df = persons_df_2.groupBy('CRASH_ID').agg(SF.some('is_female').alias('having_female')).filter(SF.col('having_female') == False).select('CRASH_ID')

    accidents_without_females_df_1 = units_df_2.join(accidents_without_females_df, on=['CRASH_ID'], how='inner')

    statewise_accidents_df = accidents_without_females_df_1.groupBy(SF.col('VEH_LIC_STATE_ID').alias('state')).agg(SF.count('CRASH_ID').alias('total_accidents'))

    top_state_by_accidents_df = statewise_accidents_df.orderBy(SF.col('total_accidents').desc()).limit(1)

    result_df = top_state_by_accidents_df

    write_df(spark, result_df, config, 'analytics_5')

    result = [(str(i.state), str(i.total_accidents)) for i in result_df.collect()]
    logger.info(f'State having the highest number of accidents in which females are not involved = {result}')
