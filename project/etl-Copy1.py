import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, year, month, dayofweek, hour, weekofyear, dayofmonth, monotonically_increasing_id, from_unixtime
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws_config']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_config']['AWS_SECRET_ACCESS_KEY']
#os.environ['AWS_DEFAULT_REGION']=config['aws_config']['AWS_DEFAULT_REGION']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/*/*/*.json')
    
    
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    song_fields = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude",
                      "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong') 

    # extract columns for users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = log_df.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.timestamp))

    # extract columns to create time table
    log_df = log_df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))

    time_table = log_df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")
    
    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    songs_logs = log_df.join(songs_df, (log_df.song == songs_df.title))

    # extract columns from joined song and log datasets to create songplays table 
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name))
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.ts, 'left'
    ).drop(artists_songs_logs.year)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month'),
    ).repartition("year", "month")
    
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data, 'songplays')
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://vp-spark-471182/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()









  # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("tbllog_data")
    
    # filter by actions for song plays
    #df = spark.sql("SELECT distinct userId, firstName, lastName, gender, level FROM log_data WHERE page='NextSong'")

    # extract columns for users table    
    users_table = spark.sql("SELECT distinct userId, firstName, lastName, gender, level FROM tbllog_data WHERE page='NextSong'")
    
    # write users table to parquet files
    users_table.write.parquet(output_data+ "/users/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour('timestamp')) \
    .withColumn('day', hour('timestamp')) \
    .withColumn('week', hour('timestamp')) \
    .withColumn('month', hour('timestamp')) \
    .withColumn('year', hour('timestamp')) \
    .withColumn('weekday', hour('timestamp'))
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionedBy('year', 'month').parquet(output_data+'/time/',mode='overwrite')
    

    # read in song data to use for songplays table
    songs_df = spark.sql('SELECT DISTINCT song_id, artist_id, artist_name FROM tblsong_df')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner") \
        .distinct() \
        .select('start_time', 'userId', 'level', 'sessionId', \
                'location', 'userAgent', 'song_id', 'artist_id' \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month 
    #songplays_table.write.partitionedBy('year', 'month').parquet(output_data+'/songplays/',mode='overwrite')
