# Introduction:

#### A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

#### As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

#### I'll be able to test your database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare my results with their expected results.

#### In this project, I'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. I'll deploy this Spark process on a cluster using AWS.

# DataSets:

#### I'll be working with two datasets that reside in S3. Here are the S3 links for each:

#### Song data: s3://udacity-dend/song_data
#### Log data: s3://udacity-dend/log_data

# How to run the project:

#### Type python etl.py in terminal.
