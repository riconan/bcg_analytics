#### BCG Analytics

##### Usage

1. Extract the csv.zip in the input folder

2. configure the config.json in the root folder (I/O paths and file formats). Only give absolute paths to the I/O destinations

3. submit the main.py to spark with the command line option as 1-10 or 'all'

4. submit job will execute the necessary analytics and the output will be available in the configured destination

5. time taken for each analytics along with the output is present in the main.log file that gets generated upon execution (INFO logs will be the output).  

---------

##### Notes

analytics package contains individual analytics as a separate module. 

logger defined in the utils.logger module is the preferred way of logging across the project. dest of the log file can be configured there.

all the python code uses only the standard library that comes pre-packaged with python. Thus didn't add any requirements file.  

---------
##### Test System Spec

OS:	Ubuntu 24.04.1 LTS (x86-64)  (Codename: noble)   
Java Version: 17  
Spark Version: 3.5.4  
Spark Downloaded from: https://downloads.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3-scala2.13.tgz  
Python Version: 3.13.1
