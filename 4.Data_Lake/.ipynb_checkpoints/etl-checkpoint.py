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


def process_song_data(spark, input_data, output_data):
    """
    This function load the song data from S3 and make the songs and artists dimension table to parquet file.
    Input: spark session, the file path of input data, and the file path of the output data.
    Return: None.
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = input_data+'/song_data/*/*/*/*.json'
    
    # read song data file
    print('Read song data file.')
    df = spark.read.json(song_data).dropDuplicates()
    print('Read song data file done.')
    print('---------------------------')

    # extract columns to create songs table
    print('Create song table.')
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data,'dimsongs.parquet'), partitionBy=['year','artist_id'])
    print('Create song table done.')
    print('--------------------------')

    # extract columns to create artists table
    print('Create artist table.')
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'dimartists.parquet'))
    print('Create artist table done.')
    print('-------------------------')


def process_log_data(spark, input_data, output_data):
    """
    This function load the log data from S3 and make the users, time dimension table and songplay fact table to parquet file.
    Input: spark session, the file path of input data, and the file path of the output data.
    Return: None.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    #log_data = input_data+'/log_data/*.json'

    # read log data file
    print('Load log data.')
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')
    print('Load log data done.')
    print('---------------------')

    # extract columns for users table
    print('Make users table.')
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'dimusers.parquet'))
    print('Make users table done.')
    print('--------------------------')

    # create timestamp column from original timestamp column
    print('Make time table.')
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn('start_time', get_datetime('timestamp'))
    
    # extract columns to create time table
    time_table = df.withColumn('hour', hour('start_time'))\
                   .withColumn('day', dayofmonth('start_time'))\
                   .withColumn('week',weekofyear('start_time'))\
                   .withColumn('month',month('start_time'))\
                   .withColumn('year',year('start_time'))\
                   .withColumn('weekday', dayofweek('start_time'))\
                   .select('start_time','hour','day','week','month','year','weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data,'dimtime.parquet'))
    print('Make time table done.')
    print('-----------------------')

    # read in song data to use for songplays table
    print('Make songplay table.')
    song_df = spark.read.parquet(os.path.join(output_data,'dimsongs.parquet'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.title==df.song))\
                        .withColumn('songplay_id', monotonically_increasing_id())\
                        .select('songplay_id', 'start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data,'factsongplay.parquet'))
    print('Make songplay table done.')
    print('----------------------------')


def main():
    """
    This function create a new spark session and run process_song_data function and process_log_data function.
    Input: None.
    Return: None.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakeprojectbokai/"
    #input_data="/home/workspace/data"
    #output_data="/home/workspace/data/output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
