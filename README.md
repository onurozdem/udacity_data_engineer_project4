# Project: Data Lake
This repository was created as part of my training on Udacity. For detailed information, you can check the https://www.udacity.com/course/data-engineer-nanodegree--nd027 link.

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, from S3 processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Datasets
This project working with two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data/
Log data: s3://udacity-dend/log-data/

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
   * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
1. users - users in the app
   * user_id, first_name, last_name, gender, level
2. songs - songs in music database
   * song_id, title, artist_id, year, duration
3. artists - artists in music database
   * artist_id, name, location, lattitude, longitude
4. time - timestamps of records in songplays broken down into specific units
   * start_time, hour, day, week, month, year, weekday

## ETL Pipeline
In this project, the datas is located on S3 buckets. You must run just etl process, loading data from S3 and processed data transform to S3 again in dimensional format.

Dimensional data target: s3://udacity-dend/project4-out/

# Project Template
To get started with the project, you can find files at repository's main branch. You can clone or download the project template files from the repository, if you'd like to develop your project locally.

Alternatively, you can download the template files in the Resources tab in the classroom and work on this project on your local computer.

The project template includes four files:

  * `etl.py` reads data from S3, processes that data using Spark, and writes them back to S3
  * `dl.cfg` contains your AWS credentials
  * `README.md` provides discussion on your process and decisions


## Project Running
* You must fill credential file(dl.cfg) before run etl script.
* You must start just etl process:
  > python etl.py

After, this step, you complete first ETL process. If you want to continuosly or scheduled running this ETL process, you can use Cron Job or Airflow methods. 



### Cron Job:
https://en.wikipedia.org/wiki/Cron

### Airflow Scheduling:
https://airflow.apache.org/docs/1.10.1/scheduler.html#:~:text=The%20Airflow%20scheduler%20monitors%20all,whether%20they%20can%20be%20triggered.
https://airflow.apache.org/docs/stable/scheduler.html

 

