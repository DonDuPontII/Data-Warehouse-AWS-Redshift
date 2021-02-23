import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events(
                                      artist varchar distkey,
                                      auth varchar,
                                      firstName varchar,
                                      gender varchar,
                                      itemInSession int,
                                      lastName varchar,
                                      length decimal,
                                      level varchar,
                                      location varchar,
                                      method varchar,
                                      page varchar,
                                      registration decimal,
                                      sessionId bigint,
                                      song varchar,
                                      status int,
                                      ts bigint sortkey,
                                      userAgent varchar,
                                      userId int)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                     num_songs int,
                                     artist_id varchar NOT NULL,
                                     artist_latitude varchar,
                                     artist_longitude varchar,
                                     artist_location varchar,
                                     artist_name varchar NOT NULL distkey,
                                     song_id varchar NOT NULL sortkey,
                                     title varchar NOT NULL,
                                     duration decimal NOT NULL,
                                     year int NOT NULL)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                                songplay_id int IDENTITY(0,1),
                                start_time decimal NOT NULL REFERENCES time sortkey,
                                user_id int NOT NULL REFERENCES users,
                                level varchar,
                                song_id varchar REFERENCES songs distkey,
                                artist_id varchar REFERENCES artists,
                                session_id int NOT NULL,
                                location varchar,
                                user_agent varchar, 
                                PRIMARY KEY(songplay_id))""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                            user_id int sortkey,
                            first_name varchar NOT NULL,
                            last_name varchar NOT NULL,
                            gender varchar,
                            level varchar NOT NULL,
                            PRIMARY KEY(user_id))
                            diststyle all""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                            song_id varchar sortkey distkey,
                            title varchar NOT NULL,
                            artist_id varchar NOT NULL,
                            year int NOT NULL,
                            duration decimal NOT NULL,
                            PRIMARY KEY(song_id))""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                              artist_id varchar sortkey,
                              name varchar NOT NULL,
                              location varchar,
                              latitude varchar,
                              longitude varchar,
                              PRIMARY KEY(artist_id))
                              diststyle all""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time decimal sortkey,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year int,
                            weekday int,
                            PRIMARY KEY(start_time))
                            diststyle all""")

# STAGING TABLES
# Source: https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto ignorecase' REGION 'us-west-2'
    """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto ignorecase' REGION 'us-west-2'
    """).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(
                                start_time,
                                user_id,
                                level,
                                song_id,
                                artist_id,
                                session_id,
                                location,
                                user_agent) 
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""")

user_table_insert = ("""INSERT INTO users(
                            user_id,
                            first_name,
                            last_name,
                            gender,
                            level) 
                        VALUES(%s,%s,%s,%s,%s)""")

song_table_insert = ("""INSERT INTO songs(
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration)
                        VALUES(%s,%s,%s,%s,%s)""")

artist_table_insert = ("""INSERT INTO artists(
                              artist_id,
                              name,
                              location,
                              latitude,
                              longitude) 
                          VALUES(%s,%s,%s,%s,%s)""")

time_table_insert = ("""INSERT INTO time(
                            start_time,
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday) 
                        VALUES(%s,%s,%s,%s,%s,%s,%s)""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]

# Table list used to drop tables
table_list = ['staging_events',
              'staging_songs',
              'songplays',
              'users',
              'songs',
              'artists',
              'time']
