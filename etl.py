import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["aws"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["aws"]['AWS_SECRET_ACCESS_KEY']
#os.environ['AWS_REGION']=config["aws"]['AWS_REGION']

def create_spark_session():
    """
    This function create Spark Session for usage of Spark.

    Parameters:
    This function don't take any argument.

    Returns:
    SparkSession: Session of Spark env. connection 

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function prepare songs and artist tables data and load to AWS S3 bucket on parquet format(without data duplication).

    Parameters:
    spark (SparkSession): Session of Spark env. connection 
    input_data (string): Description of arg1
    output_data (string): Description of arg1
    """
    # get filepath to song data file
    song_data = "{}/song_data/*/*/*".format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    songs_table = songs_table.dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs_table/"), mode = "overwrite", partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "name", "location", "lattitude", "longitude"])
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists_table/"), mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function prepare users, time and songplays tables data and load to AWS S3 bucket on parquet format(without data duplication).

    Parameters:
    spark (SparkSession): Session of Spark env. connection 
    input_data (string): Description of arg1
    output_data (string): Description of arg1

    """
    # get filepath to log data file
    log_data = "{}/log-data/*".format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["user_id", "first_name", "last_name", "gender", "level"])
    users_table = users_table.dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users_table/"), mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime').withColumn('start_time', actions_df.datetime) \
                                      .withColumn('hour', hour('datetime')) \
                                      .withColumn('day', dayofmonth('datetime')) \
                                      .withColumn('week', weekofyear('datetime')) \
                                      .withColumn('month', month('datetime')) \
                                      .withColumn('year', year('datetime')) \
                                      .withColumn('weekday', dayofweek('datetime'))
    time_table = time_table.dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode = "overwrite", partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_data = "{}/song_data/*/*/*".format(input_data)
    song_df = spark.read.json(song_data) 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.alias('logs').join(song_df.alias('songs'),col('log.artist') == col('songs.artist_name'))
    
    #songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
    songplays_table = songplays_table.select(col('log_df.datetime').alias('start_time'),
                                             col('log_df.userId').alias('user_id'),
                                             col('log_df.level').alias('level'),
                                             col('song_df.song_id').alias('song_id'),
                                             col('song_df.artist_id').alias('artist_id'),
                                             col('log_df.sessionId').alias('session_id'),
                                             col('log_df.location').alias('location'), 
                                             col('log_df.userAgent').alias('user_agent'),
                                             year('log_df.datetime').alias('year'),
                                             month('log_df.datetime').alias('month')).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays_table/"), mode = "overwrite", partitionBy=["year", "month"])


def main():
    """
    This function is main block of etl process. All process execute sequentially. Don't take any argument and return value.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/project4-out/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
