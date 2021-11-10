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

def process_i94_data(spark, input_data, output_data):
    """
    This function load the i94 data from local directory and make the fact table to parquet file.
    Input: spark session, the file path of input data, and the file path of the output data.
    Return: None.
    """
    # get filepath to i94 data file
    i94_data = input_data
    
    # read i94 data file
    print('Read i94 data file.')
    df = spark.read.load(i94_data).dropDuplicates()
    print('Read i94 data file done.')
    print('---------------------------')

    # extract columns to create fact table
    print('Create fact table.')
    fact_table = df.select("cicid", "i94mon", "i94res" ,"i94port", "i94addr", "visatype", "dtadfile", "gender").dropDuplicates()
    print('Create fact table done.')
    print('--------------------------')
    
    # write fact table to parquet files partitioned by visatype
    print('Write fact table to S3.')
    fact_table.write.parquet(os.path.join(output_data,'fact_table.parquet'), partitionBy=['visatype'])
    print('Create fact table done.')
    print('--------------------------')

def main():
    """
    This function create a new spark session and run process_i94_data function.
    Input: None.
    Return: None.
    """
    spark = create_spark_session()
    output_data = "s3a://capstone-bokai/"
    input_data = './sas_data'
    
    process_i94_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
