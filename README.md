A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

This proeject consists of 2 other main files, the 'etl.py' and the 'dl.cfg' files.

Each file does the following:

1-etl.py- running this file using python will:

-start the spark session

-load and process the song data 
    -create the song's data dataframe
    -extract song table 
    -extract artists table 
    
-load and process the log data
    -create the log's data Dataframe
    -extract the user table
    -extract the time table
    -extract the songplays table by joining both dataframes
    
-copy extracted data into users S3 bucket

-stop the spark session

2-dl.cfg- This file contains the credntial for the user on AWS so that we can connect to the sparkify's S3 bucket, copy the data from the thet storage bucket into our Spark session and creating then into another S3 bucket created by the user.

Here we used the fact vs dimention star schema Database with the songplaystable as the fact table and the 4 other tables (users, songs, artists and time) as the dimension tables for fast aggregation calculations and allowing for further JOINs to satisfy further analytical queries.

Below is the exact schema used:

-Fact Table- (songplays)

coloumns = (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

-Dimension Tables-

users - users in the app
coloumns = (user_id, first_name, last_name, gender, level)

songs - songs in music database
coloumns = (song_id, title, artist_id, year, duration)

artists - artists in music database
coloumns = (artist_id, name, location, lattitude, longitude)

time - timestamps of records in songplays broken down into specific units
coloumns = (start_time, hour, day, week, month, year, weekday)

refrences:
https://sparkbyexamples.com/spark/spark-read-and-write-json-file/
https://stackoverflow.com/questions/51689460/select-specific-columns-from-spark-dataframe
https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.monotonically_increasing_id.html
https://sparkbyexamples.com/spark/spark-extract-hour-minute-and-second-from-timestamp/
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-joins.html
https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.dropDuplicates.html#pyspark.sql.DataFrame.dropDuplicates