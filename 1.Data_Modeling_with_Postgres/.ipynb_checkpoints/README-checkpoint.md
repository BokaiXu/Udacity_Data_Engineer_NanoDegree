# Introduction:

#### A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. This Postgres database with tables were designed to optimize queries on song play analysis.

# Project Description:

#### In this project, a star schema was created with four dimension tables and one fact table.

# Fact table:

#### songplays - records in log data associated with song plays i.e. records with page NextSong. Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.

# Dimension tables: 

#### users - users in the app. Columns: user_id, first_name, last_name, gender, level. 

#### songs - songs in music database. Columns: song_id, title, artist_id, year, duration

#### artists - artists in music database. Columns: artist_id, name, location, latitude, longitude

#### time - timestamps of records in songplays broken down into specific units. Columns: start_time, hour, day, week, month, year, weekday

#### Two json datasets containing the song information and user activities were converted to pandas dataframe and recorded into the fact table and dimension tables through a ELT pipeline.

# Files in the project:

#### create_table.py delete the talbes if exist and create new tables.
#### etl.py write data into the tables one by one.
#### test.ipynb test whether the info has been written into the database.
#### sql_queries.py is the helper funtion that was used in etl.py and create_tables.py.
#### etl.ipynb is the Jupyter notebook that used to test the function of etl.py.

# How to run the database:

#### Type "python create_tables.py" to create the database.
#### Type "python etl.py" to write info into the database.
#### Use test.ipynb to test whether the data was correctly written into the data base.