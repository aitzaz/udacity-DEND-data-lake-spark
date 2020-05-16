import configparser
import logging
import os

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, udf
from pyspark.sql.types import TimestampType

# Set logging config
logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

# Read aws config
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('aws', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('aws', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Returns a new or existing spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads songs data in a spark dataframe; creates two dataframes from the main dataframe as songs_table
    and artists_table, drop duplicates in those and save those dataframes in parquet format on the output path.

    :param spark: Spark session object
    :param input_data: S3 or local dir containing song data
    :param output_data: Path for parquet output files
    """
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"

    # read song data file
    logger.info("Reading song data json files")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    songs_table = songs_table.dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    logger.info('Writing song table partitioned by year and artist_id in parquet format')
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "/tbl_songs.parquet")

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artists_table = artists_table \
        .withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude') \
        .dropDuplicates()

    # write artists table to parquet files
    logger.info('Writing artists table in parquet format')
    artists_table.write.parquet(output_data + '/tbl_artists.parquet')


def process_log_data(spark, input_data, output_data):
    """
    Reads logs data in a dataframe which is then used to create new dataframes for creating users and time tables.
    Reads songs data and join it with logs dataframe to create a data for songplays table.
    Drop duplicates, rename columns and finally saves all tables in parquet format.

    :param spark: Spark session object
    :param input_data: S3 or local dir containing song data
    :param output_data: Path for parquet output files
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"  # S3 dir structure
    # log_data = input_data + "log_data/*.json"           # local dir structure

    # read log data file
    logger.info('Reading log data json files')
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table
    users_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    users_table = users_table \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('firstName', 'first_name') \
        .withColumnRenamed('lastName', 'last_name') \
        .dropDuplicates()

    # write users table to parquet files
    logger.info('Writing users table in parquet format')
    users_table.write.parquet(output_data + '/tbl_users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))

    # create datetime columns from derived start_time column
    df = df.withColumn('hour', hour(df.start_time))
    df = df.withColumn('day', dayofmonth(df.start_time))
    df = df.withColumn('week', weekofyear(df.start_time))
    df = df.withColumn('month', month(df.start_time))
    df = df.withColumn('year', year(df.start_time))
    df = df.withColumn('weekday', dayofweek(df.start_time))

    # extract columns to create time table
    time_table = df[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
    time_table = time_table.dropDuplicates()

    # write time table to parquet files partitioned by year and month
    logger.info('Writing time table partitioned by year and month in parquet format')
    time_table.write.partitionBy('year', 'month').parquet(output_data + '/tbl_time.parquet')

    # read in song data to use for songplays table
    logger.info("Reading song data for join")
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    song_df = song_df.withColumnRenamed('year', 'song_year')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, 'inner')
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())
    songplays_table = songplays_table[['songplay_id', 'start_time', 'userId', 'level', 'song_id',
                                       'artist_id', 'sessionId', 'location', 'userAgent', 'month', 'year']]
    songplays_table = songplays_table \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('sessionId', 'session_id') \
        .withColumnRenamed('userAgent', 'user_agent')

    # write songplays table to parquet files partitioned by year and month
    logger.info('Writing songplays table partitioned by year and month in parquet format')
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + '/tbl_songplays.parquet')


def main():
    """
    Main function to drive the script.
    """
    spark = create_spark_session()

    # S3 data paths
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://your-output-bucket/"

    # Local data paths
    #     input_data = "data/"
    #     output_data = "output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    logger.info("Job completed!")


if __name__ == "__main__":
    main()
