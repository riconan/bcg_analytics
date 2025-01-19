import pyspark.sql
from pathlib import Path

from utils.logging import logger

__all__ = ['read_file', 'write_df']


def read_file(spark: pyspark.sql.SparkSession, config : dict, file_id : str) -> pyspark.sql.DataFrame:
    """
    Description:
        Reads csv/parquet as configured
    
    Arguments:
        spark: A pyspark.sql.SparkSession
        config: should have each file's read format, abspath, stem
        file_id: file_id to be searched in the config

    Returns:
        the file contents as pyspark.sql.DataFrame
    
    """
    file_config = config['input'][file_id]
    file_format = file_config['file_format']
    base_path = file_config['abs_basepath']
    stem = file_config['stem']
    file_path = str(Path(base_path) / stem)


    if(file_format == 'csv'):
        csv_header = file_config['header']
        csv_sep = file_config['sep']
        return spark.read.csv(path=file_path, header=csv_header, sep=csv_sep, inferSchema=False) # not inferring schema to keep the data in raw form
    
    elif(file_format == 'parquet'):
        return spark.read.parquet([file_path])
    else:
        raise NotImplementedError(f'given file format {file_format} cannot be read. only supported formats are csv and parquet')
    


def write_df(spark: pyspark.sql.SparkSession, df: pyspark.sql.DataFrame, config : dict, file_id : str) -> str:

    """
    Description:
        writes the spark dataframe as configured
    
    Arguments:
        spark: A pyspark.sql.SparkSession
        df: the dataframe which should be written
        config: should have each file's read format, abspath
        file_id: file_id to be searched in the config

    Returns:
        the path where the data was written
    
    """

    file_config = config['output'][file_id]
    file_format = file_config['file_format']
    abs_path = file_config['abs_path']
    write_mode = 'overwrite'
    output_path = str(Path(abs_path))

    # to avoid overwrite root folders
    assert len(abs_path) > 5

    # warning for overwrite
    if(write_mode == 'overwrite'):
        logger.warning(msg=f'{output_path} will be overwritten')

    if(file_format == 'parquet'):
        df.fillna('NA').repartition(1).write.option("maxRecordsPerFile", 10000).format(file_format).mode(write_mode).save(output_path)
    elif(file_format == 'csv'):
        df.fillna('NA').repartition(1).write.option("maxRecordsPerFile", 10000).format(file_format).options(header=True, sep=',').mode(write_mode).save(output_path)
    else:
        raise NotImplementedError(f'given file format {file_format} cannot be written. only supported format is parquet')
    
    return output_path