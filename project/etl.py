import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CONFIG']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CONFIG']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    df.createOrReplaceTempView('tblsong_data')
    
    # extract columns to create songs table
    songs_table = spark.sql('SELECT DISTINCT song_id, title, artist_id, year, duration from tblsong_data')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql('SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from tblsong_data')
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')
    
    df.createOrReplaceTempView("tblsong_df")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/2018/11/*.json")
    
    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("tbllog_data")
    
   # filter by actions for song plays
    #df = spark.sql("SELECT distinct userId, firstName, lastName, gender, level FROM log_data WHERE page='NextSong'")

    # extract columns for users table    
    users_table = spark.sql("SELECT distinct userId, firstName, lastName, gender, level FROM tbllog_data WHERE page='NextSong'")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

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
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")
    
    # read in song data to use for songplays table
    songs_df = spark.sql('SELECT DISTINCT song_id, artist_id, artist_name FROM tblsong_df')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner" \
        ).distinct() \
        .select('start_time', 'userId', 'level', 'sessionId', \
                'location', 'userAgent', 'song_id', 'artist_id') \
        .withColumn('songplay_id', monotonically_increasing_id())
    # write songplays table to parquet files partitioned by year and month 
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data, 'songplays')
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://vp-spark-471182/"
    
    process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
