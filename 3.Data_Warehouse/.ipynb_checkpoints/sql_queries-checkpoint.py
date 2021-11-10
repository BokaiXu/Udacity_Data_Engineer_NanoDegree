import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factsongplays"
user_table_drop = "DROP TABLE IF EXISTS dimusers"
song_table_drop = "DROP TABLE IF EXISTS dimsongs"
artist_table_drop = "DROP TABLE IF EXISTS dimartists"
time_table_drop = "DROP TABLE IF EXISTS dimtime"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events 
                              (artist varchar,
                               auth varchar,
                               firstName varchar,
                               gender varchar,
                               itemInSession int,
                               lastName varchar,
                               length float,
                               level varchar,
                               location varchar,
                               method varchar,
                               page varchar,
                               registration bigint,
                               sessionId int,
                               song varchar,
                               status int,
                               ts timestamp,
                               userAgent varchar,
                               userId int
                              );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                              (num_songs int, 
                               artist_id varchar,
                               artist_latitude float,
                               artist_longitude float,
                               artist_location varchar,
                               artist_name varchar,
                               song_id varchar,
                               title varchar,
                               duration float,
                               year int
                              );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factsongplays 
                            (songplay_id int identity(0,1) PRIMARY KEY,
                             start_time timestamp SORTKEY,
                             user_id int,
                             level varchar,
                             song_id varchar,
                             artist_id varchar,
                             session_id int,
                             location varchar,
                             user_agent varchar
                            );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dimusers 
                        (user_id int PRIMARY KEY DISTKEY,
                         first_name varchar,
                         last_name varchar,
                         gender varchar,
                         level varchar
                        );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimsongs 
                        (song_id varchar PRIMARY KEY SORTKEY,
                         title varchar,
                         artist_id varchar,
                         year int,
                         duration float
                        );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimartists
                          (artist_id varchar PRIMARY KEY SORTKEY,
                           name varchar,
                           location varchar,
                           latitude float,
                           longitude float
                          );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimtime
                        (start_time timestamp PRIMARY KEY SORTKEY,
                         hour int,
                         day int,
                         week int,
                         month int,
                         year int,
                         weekday int
                        );
""")

LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')
ARN=config.get('IAM_ROLE','ARN')

# STAGING TABLES

staging_events_copy = ("""COPY staging_events from {}
                          CREDENTIALS 'aws_iam_role={}'
                          COMPUPDATE OFF region 'us-west-2'
                          FORMAT AS json {}
                          TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs from {}
                          CREDENTIALS 'aws_iam_role={}'
                          COMPUPDATE OFF region 'us-west-2'
                          FORMAT AS json 'auto'; 
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO factsongplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT a.ts, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, a.location, a.userAgent
FROM staging_events a
JOIN staging_songs b
ON a.artist=b.artist_name AND a.song=b.title
WHERE a.page='NextSong';
""")

user_table_insert = ("""INSERT INTO dimusers (user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId is NOT NULL;
""")

song_table_insert = ("""INSERT INTO dimsongs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id is NOT NULL;
""")

artist_table_insert = ("""INSERT INTO dimartists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id is NOT NULL;
""")

time_table_insert = ("""INSERT INTO dimtime (start_time, hour, day, week, month, year, weekday)
SELECT ts, EXTRACT(hour from ts), EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(month from ts), EXTRACT(year from ts), EXTRACT(weekday from ts)
FROM staging_events
WHERE ts is NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
