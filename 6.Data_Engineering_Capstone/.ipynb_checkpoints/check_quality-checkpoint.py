import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, from_unixtime, monotonically_increasing_id
from pyspark.sql.types import IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates a spark session.
    Input: None.
    Return: A new spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def check_quality_2(spark, input_data):
    """
    This function load the i94 data from S3 and check whether it contains rows.
    Input: spark session, the file path of input data.
    Return: None.
    """
    # get filepath to i94 data file
    i94_data = input_data
    
    # read i94 data file
    print('Read i94 data file.')
    df = spark.read.load(i94_data)
    print('Read i94 data file done.')
    print('---------------------------')
    
    # Run quality check
    print('Check i94 data quality.')
    try:
        df.show(n=1)
        print('Check i94 data quality done.')
    except:
        print('Check i94 data quality fail.')
    print('--------------------------')

def main():
    """
    This function create a new spark session and run check_quality_2 function.
    Input: None.
    Return: None.
    """
    spark = create_spark_session()
    input_data = "s3a://capstone-bokai/fact_table.parquet"
    
    check_quality_2(spark, input_data)


if __name__ == "__main__":
    main()
