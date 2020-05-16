# Data Lake with Spark

## Project Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Description
This project intends to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Run Guide
Make sure that you have `pyspark` installed locally or in a virtualenv.

- Update `dl.cfg` with an IAM user credentials having access to read and write on S3.
- Run `python etl.py` on terminal to run the script.

### Local Run on sample dataset
To run the script on sample data only requires two more steps:

- In `main` function, comment S3 paths and uncomment local data paths below that.
- In `process_log_data` function, comment S3 path and uncomment local path (nesting dir levels are different in local data dir vs S3 bucket).

Note: To re-run locally, you need to remove content of `output` directory using `rm -rf 'path/to/output/'`. 

## Schema for Song Play Analysis
Using the song and log datasets, a star schema is created and optimized for queries on song play analysis. This includes the following tables.

### Fact Table
**tbl_songplays** - records in log data associated with song plays i.e. records with page NextSong

_songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

### Dimension Tables
**tbl_users** - users in the app

_user_id, first_name, last_name, gender, level_

**tbl_songs** - songs in music database

_song_id, title, artist_id, year, duration_

**tbl_artists** - artists in music database

_artist_id, name, location, lattitude, longitude_

**tbl_time** - timestamps of records in songplays broken down into specific units

_start_time, hour, day, week, month, year, weekday_

## Description of Project Files 

**dl.cfg**: Contains IAM user credentials used by `pyspark` to read/write files on S3.

**etl.py**: Contains functions to process songs and logs data from S3 by loading in Spark dataframes, removing duplicates, manipulating columns and saving dataframes on S3 in `parquet` format.
