import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format

# Read in config from dl.cfg
config = configparser.ConfigParser()
config.read('dl.cfg')

def create_spark_session():
    """Create the spark session object
    
    Returns:
        SparkSession -- spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_dir):
    """Read in song data from an input source, split the data into relevant 
    tables, and write to partioned parquet tables
    
    Arguments:
        spark {SparkSession} -- spark session object
        input_data {string} -- path of the input song data
        output_dir {string} -- path where output data should be stored
    
    Returns:
        DataFrame -- Pyspark dataframe with all song related data
    """
    # read song data file
    df = spark.read.json(input_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet("%s/songs.parquet"%output_dir, 'overwrite')

    # extract columns to create artists table
    artists_table = df.select("artist_id", 
                              col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_longitude").alias("longitude"))
    
    # write artists table to parquet files
    artists_table.write.parquet("%s/artists.parquet"%output_dir, 'overwrite')
    
    return df


def process_log_data(spark, input_data, output_dir):
    """Read in log data from source, split out user and songplay data,
    and write to partitioned parquet files
    
    Arguments:
        spark {SparkSession} -- Spark session object
        input_data {string} -- Path to log data files
        output_dir {string} -- Path where output tables will be stored
    
    Returns:
        DataFrame -- Pyspark DataFrame containing log-related data
    """
    # read log data file
    df = spark.read.json(input_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    artists_table = df.select(col("UserId").alias("user_id"),
                             col("FirstName").alias("first_name"),
                             col("LastName").alias("last_name"),
                             "gender",
                             "level")
    
    # write users table to parquet files
    artists_table.write.parquet("%s/users.parquet"%output_dir, 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts), )
                          
    # extract columns to create time table
    df = df.withColumn("hour", hour("start_time"))
    df = df.withColumn("day", dayofweek("start_time"))
    df = df.withColumn("week", weekofyear("start_time"))
    df = df.withColumn("month", month("start_time"))
    df = df.withColumn("year", year("start_time"))
    df = df.withColumn("weekday", dayofweek("start_time"))
    time_table = df.select("start_time","hour","day","week","month","year","weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet("%s/time.parquet"%output_dir, 'overwrite')

    return df

def process_songplay_data(spark, song_df, log_df, output_dir):
    """Combine the song and log data, extract songplay data and output
    partitioned songplay parquet files
    
    Arguments:
        spark {SparkSession} -- spark session object
        song_df {DataFrame} -- spark data frame containing song related data
        log_df {DataFrame} -- spark data frame containing log related data
        output_dir {string} -- path where output tables will be stored
    """
    # Join song and log data on artist and song title
    df = log_df.join(song_df, (log_df.artist == song_df.artist_name) & (log_df.song == song_df.title), how='left')
    
    # Add a songplay id column to data
    df = df.withColumn("songplay_id", monotonically_increasing_id())
    
    # write time table to parquet files partitioned by year and month
    df.select("songplay_id",
              "start_time",
              col("userId").alias("user_id"),
              "level",
              "song_id",
              "artist_id",
              col("sessionId").alias("session_id"),
              "location",
              "useragent",
              log_df["year"],
              "month").write.partitionBy("year","month").parquet("%s/songplay.parquet"%output_dir, 'overwrite')

def main(data_loc):
    """ Main entry point for the ETL process
    
    Arguments:
        data_loc {string} -- Where we will get the data location from in config,
                             either 'AWS' for AWS S3 Bucket or 'LOCAL' for the
                             data files on the local directory.
    """
    
    if data_loc == 'AWS':
        os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    spark = create_spark_session()

    input_song_data = config[data_loc]['INPUT_SONG_DATA']
    input_log_data = config[data_loc]['INPUT_LOG_DATA']
    output_dir = config[data_loc]['OUTPUT_DATA']

    song_df = process_song_data(spark, input_song_data, output_dir)    
    log_df = process_log_data(spark, input_log_data, output_dir)
    
    process_songplay_data(spark, song_df, log_df, output_dir)


if __name__ == "__main__":
    # Pass 'AWS' to main in order to get input data and store output to AWS S3 Bucket.
    # Pass 'LOCAL' to main in order to get input data and store output on local disk.
    main('AWS')
