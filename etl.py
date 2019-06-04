import os
from datetime import datetime

import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import year, month
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('LOGIN', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('LOGIN', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates the SparkSession object.
    :return:  The SparkSession object.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Runs the ETL process for the songs folder. It creates a parquet output for
    the songs and artists tables.
    :param spark:
        SparkSession object.
    :param input_data:
        Folder containing the input data for songs and log events.
    :param output_data:
        Folder where the output of the ETL process is going to be placed.
    """
    song_schema = StructType([
        StructField('num_songs', IntegerType(), True),
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', FloatType(), True),
        StructField('artist_longitude', FloatType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('duration', FloatType(), True),
        StructField('year', IntegerType(), True)
    ])
    song_data = input_data + 'song_data/*/*/*/*'

    df = spark.read.json(song_data, schema=song_schema)
    df.createOrReplaceTempView("staging_song")

    songs_table = spark.sql('''
        select distinct song_id as song_id, 
            title as title, 
            artist_id as artist_id, 
            year as year, 
            duration as duration
        from staging_song
        where song_id is not null
    ''')
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/')

    artists_table = spark.sql('''
        select distinct artist_id, 
            artist_name as name, 
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
        from staging_song
        where artist_id is not null
    ''')
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Runs the ETL process for the logs folder. It creates a parquet output for
    the users, time and songplays tables.
    :param spark:
        SparkSession object.
    :param input_data:
        Folder containing the input data for songs and log events.
    :param output_data:
        Folder where the output of the ETL process is going to be placed.
    """
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
        StructField('registration', FloatType(), True),
        StructField('sessionId', IntegerType(), True),
        StructField('song', StringType(), True),
        StructField('status', IntegerType(), True),
        StructField('ts', LongType(), True),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), True)
    ])
    log_data = input_data + 'log_data/*'

    df = spark.read.json(log_data, schema=log_schema)
    df.createOrReplaceTempView('staging_event')

    users_table = spark.sql('''
        select distinct userId, firstName, lastName, gender, 
            last_value(level) over (
                partition by userId
                order by ts asc
                rows between unbounded preceding and unbounded following
            ) as level
        from staging_event
        where userId is not null and page = 'NextSong'
    ''')
    users_table.write.parquet(output_data + 'users/')

    spark.udf.register('get_datetime', lambda t: datetime.fromtimestamp(t / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_with_date = spark.sql('''
        select *, get_datetime(ts) as start_time
        from staging_event
    ''')
    df_with_date.createOrReplaceTempView('staging_event_with_date')

    time_table = spark.sql('''
        select distinct start_time as start_time,
            hour(start_time) as hour,
            day(start_time) as day,
            weekofyear(start_time) as week,
            month(start_time) as month,
            year(start_time) as year,
            weekday(start_time) as weekday
        from staging_event_with_date
        where ts is not null
    ''')
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/')

    song_df = spark.read.option("mergeSchema", "true").parquet(output_data + 'songs/')
    song_df.createOrReplaceTempView('song_table')

    songplays_table = spark.sql('''
        select monotonically_increasing_id() as songplay_id, 
            se.start_time as start_time,
            se.userId as user_id,
            se.level as level,
            st.song_id as song_id,
            st.artist_id as artist_id,
            se.sessionId as session_id,
            se.location as location,
            se.userAgent as user_agent
        from staging_event_with_date as se
            left join song_table as st on (se.song = st.title)
        where se.page = 'NextSong'
    ''')

    function_names = [(year, 'year'), (month, 'month')]
    columns = [col('*')] + [f(col('start_time')).alias(name) for f, name in function_names]
    songplays_table.select(*columns)\
        .write\
        .partitionBy(*(name for _, name in function_names))\
        .parquet(output_data + 'songplays/')


def main():
    """
    Creates a SparkSession, then process the song and log data to export the result tables as parquet files.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "your-s3-output-directory"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
