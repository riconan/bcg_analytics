from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as SF
import pyspark.sql

from utils.io import read_file, write_df
from utils.logging import logger, perf_log




def _top_25_states_with_highest_offences(spark, config):

    """
    Description:
        Returnss the top 25 states with highest offences (all offences are considered)

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        pyspark.sql.DataFrame
        columns:
            VEH_LIC_STATE_ID, total_charges

    """

    charges_df = read_file(spark, config, 'charges')
    charges_df_1 = charges_df.select('CRASH_ID', 'UNIT_NBR').distinct()

    units_df = read_file(spark, config, 'units')
    units_df_1 = units_df.select('CRASH_ID', 'UNIT_NBR', 'VEH_LIC_STATE_ID').distinct()

    units_with_charges = units_df_1.join(charges_df_1, on=['CRASH_ID', 'UNIT_NBR'], how='inner')

    statewise_total_charges = units_with_charges.groupBy('VEH_LIC_STATE_ID').agg(SF.count('*').alias('total_charges'))

    result = statewise_total_charges.orderBy(SF.col('total_charges').desc()).limit(25)

    return result

def _top_10_used_vehicle_colours(spark, config):

    """
    Description:
        Returns the top 10 most used vehicle colors

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        pyspark.sql.DataFrame
        columns:
            VEH_COLOR_ID, total_use
    
    """

    units_df = read_file(spark, config, 'units')
    units_df_1 = units_df.select('CRASH_ID', 'UNIT_NBR', 'VEH_COLOR_ID').distinct()

    colorwise_count = units_df_1.groupBy('VEH_COLOR_ID').agg(SF.count('*').alias('total_use'))

    result = colorwise_count.orderBy(SF.col('total_use').desc()).limit(10)

    return result


def _driver_with_license_and_speeding_offences(spark, config):

    """
    Description:
        Returns the drivers with license with any speeding offences

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        pyspark.sql.DataFrame
        columns:
            'CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'

    Notes:
        same person present in different crashes is considered to be a separate person
    
    """

    persons_df = read_file(spark, config, 'primary_person')
    drivers_with_license = persons_df.filter((SF.trim(SF.upper(SF.col('PRSN_TYPE_ID'))) == 'DRIVER') & ~(SF.trim(SF.upper(SF.col('DRVR_LIC_TYPE_ID'))).isin('', 'NA', 'UNKNOWN'))).select('CRASH_ID', 'UNIT_NBR', 'PRSN_NBR').distinct()


    charges_df = read_file(spark, config, 'charges')
    speeding_offences = charges_df.filter(SF.col('CHARGE').rlike('.*UNSAFE SPEED.*')).drop_duplicates(['CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'])

    result = drivers_with_license.join(speeding_offences, on=['CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'], how='inner').select('CRASH_ID', 'UNIT_NBR', 'PRSN_NBR')

    return result


def analytics_10(spark, config):

    """

    Description:
        Determine the Top 5 Vehicle Makes where 
        drivers are charged with speeding related offences, 
        has licensed Drivers, 
        used top 10 used vehicle colours and 
        has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

    Arguments:
        spark : SparkSession to execute this analytics
        config : provide input and ouput config
        
    Returns:
        None
    
    Notes:
        each condition in the problem statement is applied as a successive 'and' conditon (inner join or and)

    """

    df1 = _driver_with_license_and_speeding_offences(spark, config)
    df2 = _top_10_used_vehicle_colours(spark, config)
    df3 = _top_25_states_with_highest_offences(spark, config)

    units_df = read_file(spark, config, 'units').drop_duplicates(['CRASH_ID', 'UNIT_NBR'])

    cars_df = units_df.filter(SF.upper(SF.col('VEH_BODY_STYL_ID')).rlike('.*CAR.*'))

    cars_licensed_with_high_offence_states_df = cars_df.join(df3, on=['VEH_LIC_STATE_ID'], how='inner')

    units_with_speeding_offences_df = df1.join(units_df, on=['CRASH_ID', 'UNIT_NBR'], how='inner').select('CRASH_ID', 'UNIT_NBR', 'VEH_COLOR_ID').distinct()

    speeding_offences_with_popular_color_df = units_with_speeding_offences_df.join(df2, on=['VEH_COLOR_ID'], how='inner').select('CRASH_ID', 'UNIT_NBR').distinct()

    makewise_group = cars_licensed_with_high_offence_states_df.join(speeding_offences_with_popular_color_df, on=['CRASH_ID', 'UNIT_NBR'], how='inner').groupBy('VEH_MAKE_ID')

    makewise_top_df = makewise_group.agg(SF.count('*').alias('count')).orderBy(SF.col('count').desc())
    
    result_df = makewise_top_df.limit(5).cache()

    write_df(spark, result_df, config, 'analytics_10')

    result = [str(i.VEH_MAKE_ID) for i in result_df.collect()]

    logger.info(f"""Top 5 Vehicle Makes where 
            drivers are charged with speeding related offences, 
            has licensed Drivers, 
            used top 10 used vehicle colours and 
            has car licensed with the Top 25 states with highest number of offences = {result}""")
    
