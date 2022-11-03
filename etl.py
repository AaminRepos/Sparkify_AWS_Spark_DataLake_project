import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
     - Creates and when called deploys a spark session on AWS.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    - Read the data from the provided input source where the files reside then extracts the needed columns to create
      the songs and artists tables and finally adds them into data lake or the provided output endpoint.
    
        (args)
    
        spark: spark session used on AWS to process data.
        imput_data: path to where the original data exits in.
        output_data: path to where we are sending the extracted and transformed data to.
    """
    
    # getting filepath to song data file
    song_data ="{}song_data/*/*/*/*.json".format(input_data)
    
    # reading song data file
    df = spark.read.json(song_data)

    # extracting columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duraion').dropDuplicates()
    
    # writing songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet(os.path.join(output_data, 'songs'),\
                              partitionBy('year','artist_id'))

    # extracting columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location',\
                             'artist_latitude','artist_longitude').dropDuplicates(subset=['artist_id'])
    
    # writing artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    
    """
        - Reads the data from the provided input source where the files reside then extracts the needed columns filtered by
        the action of 'NextSong' to create the artists table,extract time table from timestamp column and create the
        songplays table by joining the song data and the log data dataframs and finally adds them into data lake or
        the provided output endpoint.
        
        (args)
        
        spark: spark session used on AWS to process data
        imput_data: path to where the original data exits in.
        output_data: path to where we are sending the extracted and transformed data to
        
    """
    
    # getting filepath to log data file
    log_data ="{}log_data/*/*/*.json".format(input_data)

    # reading log data file
    df = spark.read.json(log_data)
    
    # filtering by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extracting columns for users table    
    users_table = df.select('userId','firstName','lastName',\
                              'gender', 'level').dropDuplicates(subset=['userId'])
    
    # writing users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path(output_data, 'users'))

    # creating timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df['ts']))
    
    # creating datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x / 1000)))
    df = df.withColumn('datetime', get_datetime(df['ts']))
    
    # extracting columns to create time table
    time_table = df.select('datetime').withColumn('start_time', df['datetime'])\
                                      .withColumn('hour', hour(df['datetime']))\
                                      .withColumn('day', day(df['datetime']))\
                                      .withColumn('dayofweek', dayofweek(df['datetime']))\
                                      .withColumn('week', week(df['datetime']))\
                                      .withColumn('month', month(df['datetime']))\
                                      .withColumn('year', year(df['datetime']))\
                                      .dropDuplicates(subset=['start_time'])
    
    # writing time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(os.path.join(output_data, 'time'), partitionBy('year', 'month'))

    # reading in song data to use for songplays table
    song_df = spark.read.json("{}song_data/*/*/*/*.json".format(input_data))

    # extracting columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(""" 
                                    SELECT DISTINCT
                                    monotonically_increasing_id() AS songplay_id,
                                    log.timestamp AS start_time,
                                    log.userId AS user_id,
                                    log.level AS level,
                                    song.song_id AS song_id,
                                    song.artist_id AS artist_id,
                                    log.sessionId AS session_id,
                                    log.location AS location,
                                    log.userAgent AS user_agent
                                    FROM df log
                                    JOIN song_df song
                                    ON (log.song = song.title
                                    AND log.artist = song.artist_name);
                                 """)

    # writing songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(os.path.join(output_data, 'songplays'),\
                                                    partitionBy('year', 'month'))


def main():
    
    """
     - Main function that will execute all above functions and start the spark session, read the data from the sparkify
     files from AWS S3 and then produce the datalake on another AWS S3 bucket chosen for its cost effectivness and then
     finally stop the spark session once all processing is done.
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
       
    Spark.stop()

if __name__ == "__main__":
    main()
