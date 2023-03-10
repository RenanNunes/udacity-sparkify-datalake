import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['credentials']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['credentials']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create spark session and return it"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data from s3 and upload files back in s3

    Args:
        spark: spark session
        input_data: input data s3 path
        output_data: output data s3 path
    """
    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'
#     song_data = f'{input_data}/song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(f"{output_data}/song_data")

    # extract columns to create artists table
    artists_table = df.select("artist_id",
                              df.artist_name.alias("name"),
                              df.artist_location.alias("location"),
                              df.artist_latitude.alias("latitude"),
                              df.artist_longitude.alias("longitude")).dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}/artist_data")


def process_log_data(spark, input_data, output_data):
    """Process log data from s3 and upload files back in s3

    Args:
        spark: spark session
        input_data: input data s3 path
        output_data: output data s3 path
    """
    # get filepath to log data file
    log_data = f'{input_data}/log_data/*/*/*.json'
#     log_data = f'{input_data}/log_data/2018/11/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.select("userId",
                            df.firstName.alias("first_name"),
                            df.lastName.alias("last_name"),
                            "gender",
                            "level",
                            "ts").orderBy("ts", ascending = False).coalesce(1) \
                    .dropDuplicates(["userId"]).drop("ts")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}/user_data")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda timestamp_column: datetime.fromtimestamp(timestamp_column/ 1000.0), TimestampType())
    df = df.withColumn("ts_timestamp", get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.select(df.ts_timestamp.alias("start_time"),
                           hour("ts_timestamp").alias("hour"),
                           dayofmonth("ts_timestamp").alias("day"),
                           weekofyear("ts_timestamp").alias("week"),
                           month("ts_timestamp").alias("month"),
                           year("ts_timestamp").alias("year"),
                           dayofweek("ts_timestamp").alias("weekday")).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_data}/time_data")

    # read in song data to use for songplays table
    song_df = spark.read.option("basePath", f"{output_data}/song_data").parquet(f"{output_data}/song_data/*/*/*.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.title == df.song) &
                              (song_df.duration == df.length), "left") \
                            .select(monotonically_increasing_id().alias('songplay_id'),
                                   col("ts_timestamp").alias("start_time"),
                                   col("userId").alias("user_id"),
                                   col("level"), col("song_id"), col("artist_id"),
                                   col("sessionId").alias("session_id"), col("location"),
                                   col("userAgent").alias("user_agent"),
                                   year("ts_timestamp").alias("year"),
                                   month("ts_timestamp").alias("month"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_data}/songplay_data")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-output-project"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
