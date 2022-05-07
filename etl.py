import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField, FloatType, StringType, DoubleType, IntegerType

# Enter Credentials
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Spark Session Creator

    Returns:
        SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ETL song data. Creates song and artist tables then writes the as parquet files

    Args:
        spark: spark session
        input_data: song datasource
        output_data: parquet storage

    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # set song data schema
    song_schema = StructType([
                StructField("artist_id", StringType(), False), 
                StructField('artist_latitude', FloatType(), True),
                StructField('artist_location', StringType(), True),
                StructField('artist_longitude', FloatType(), True),
                StructField('artist_name', StringType(), True),
                StructField('duration', FloatType(), True),
                StructField('num_songs', IntegerType(), True),
                StructField('year', IntegerType(), True),
                StructField('song_id', StringType(), False),
                StructField('title', StringType(), True),

            ])

    # read song data file
    df = spark.read.schema(song_schema).json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite') \
                     .partitionBy('year', 'artist_id') \
                     .parquet(output_data + 'song_table')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite') \
                       .parquet(output_data + 'artist_table')


def process_log_data(spark, input_data, output_data):
    """ETL log data. Creates user, time, and songplay tables then writes them as parquet files

    Args:
        spark: spark session
        input_data: user log datasource
        output_data: parquet storage

    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # set schema
    log_schema = StructType([
            StructField('artist', StringType(), True),
            StructField('auth', StringType(), True),
            StructField('firstName', StringType(), True),
            StructField('gender', StringType(), True),
            StructField('itemInSession', IntegerType(), True),
            StructField('lastName', StringType(), True),
            StructField('length', FloatType(), True),
            StructField('level', StringType(), True),
            StructField('location', StringType(), True),
            StructField('method', StringType(), True),
            StructField('page', StringType(), True),
            StructField('registration', StringType(), True),
            StructField('sessionId', IntegerType(), True),
            StructField('song', StringType(), True),
            StructField('status', StringType(), True),
            StructField('ts', FloatType(), True),
            StructField('userAgent', StringType(), True),
            StructField('userId', IntegerType(), False)
        ])

    # read log data file
    df = spark.read.schema(log_schema).json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite') \
               .parquet(output_data + 'users_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0).timestamp())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0), T.TimestampType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(df.ts.alias('start_time'),
                       hour('datetime').alias('hour'), 
                       dayofmonth('datetime').alias('day'),
                       weekofyear('datetime').alias('week'),
                       month('datetime').alias('month'),
                       year('datetime').alias('year'),
                       dayofweek('datetime').alias('weekday')
                       ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite') \
                    .partitionBy('year', 'month') \
                    .parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  df.join(song_df, (df.song==song_df.title) & (df.artist==song_df.artist_name) & (df.length==song_df.duration))\
                         .select(monotonically_increasing_id().alias('id'), 
                                 df.timestamp.alias('start_time'), 
                                 df['userId'].alias('user_id'),
                                 'level',
                                 'song_id',
                                 'artist_id',
                                 df['sessionId'].alias('session_id'),
                                 'location',
                                 df.userAgent.alias('user_agent'))\
                                .where(df['page']=='NextSong').dropDuplicates()
            

    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn('year', year('start_time')) \
                   .withColumn('month', month('start_time')) \
                   .write.mode('overwrite') \
                   .partitionBy('year','month') \
                   .parquet(output_data + 'songplay_table')


def main():
    """Run full ETL for song and log data

    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-demobucket-2/data_lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
