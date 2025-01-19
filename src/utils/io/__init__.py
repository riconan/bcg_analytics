"""
This module has utilites related to I/O

Methods:

    read_file:  will read csv/parquet as configured
    write_df:   writes the pyspark.sql.DataFrame to the configured destination.
                most of the write options are hardcoded here to maintain consistency across the project

"""


from ._main import *