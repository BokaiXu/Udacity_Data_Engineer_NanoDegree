# Introduction:

#### A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

#### As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

#### In this project, I'll apply what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Datasets:

#### I'll be working with two datasets that reside in S3. Here are the S3 links for each:

#### Song data: s3://udacity-dend/song_data

#### Log data: s3://udacity-dend/log_data

#### Log data json path: s3://udacity-dend/log_json_path.json

# Tables:

#### Fact Table: songplays - records in event data associated with song plays i.e. records with page NextSong, songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### Dimension Tables: users - users in the app, user_id, first_name, last_name, gender, level, songs - songs in music database, song_id, title, artist_id, year, duration, artists - artists in music database, artist_id, name, location, lattitude, longitude, time - timestamps of records in songplays broken down into specific units, start_time, hour, day, week, month, year, weekday

# How to build the database:

#### Build a cluster at aws redshift, then run python create_tables.py and python sql_queries.py.