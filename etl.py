import configparser
import psycopg2
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries
from sqlalchemy.engine import create_engine


def load_staging_tables(cur, conn):
    """
    Loads staging tables from S3 into Redshift database by executing \
    the queries in `copy_table_queries` list from sql_queries.py

    INPUTS:
    * cur - the cursor available
    * conn - database connection
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

    
def create_data_frames(cur, conn, config):
    """
    Connect to Redshift cluster
    Create data frames from the staged tables now in the Redshift database
    Clean data frames to align with table design they will be onserted into
    Place data frames into a list and return list
    
    INPUTS:
    * cur - the cursor available
    * conn - database connection
    * config - parameters to Redshift cluster
    """
    
    # Connect to Redshift cluster
    cluster_connect = create_engine("postgresql://{}:{}@{}:{}/{}".format(
        config.get('CLUSTER', 'DB_USER'),
        config.get('CLUSTER', 'DB_PASSWORD'),
        config.get('CLUSTER', 'HOST'),
        config.get('CLUSTER', 'DB_PORT'),
        config.get('CLUSTER', 'DB_NAME')))
    print(cluster_connect)
    
    # Read song data into DataFrame
    # Limiting to subset to speed up insert process
    song_df = pd.read_sql("""select distinct
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                              from staging_songs
                              limit 1000
                            """, cluster_connect)
#     print(song_df.head())
    
    # Read artist data frame
    # Make the artist_id unqiue
    # Limiting to subset to speed up insert process
    artist_df = pd.read_sql("""select distinct
                                   staging_songs.artist_id,
                                   artist_name,
                                   artist_location,
                                   artist_latitude,
                                   artist_longitude
                                from staging_songs
                                join (select  
                                       artist_id,
                                       max(song_id) as song_id
                                      from staging_songs
                                      group by artist_id) as id
                                     on staging_songs.artist_id = id.artist_id
                                         and staging_songs.song_id = id.song_id
                                limit 1000
                             """, cluster_connect)
#     print(artist_df.head())

    # Load user table
    # Remove duplicate user id by keeping most recent record
    user_df = pd.read_sql("""select distinct
                                staging_events.userid,
                                firstname,
                                lastname,
                                gender,
                                level
                            from staging_events 
                            join (select 
                                    userid, 
                                    max(ts) as ts
                                  from staging_events
                                  where page = 'NextSong'
                                  group by userid) as recent_ts
                                on staging_events.userid = recent_ts.userid
                                    and staging_events.ts = recent_ts.ts
                          """, cluster_connect)
#     print(user_df.head())
    
    # Filter by NextSong action to get song plays only
    # Limiting to subset to speed up insert process
    song_plays_df = pd.read_sql("""select * 
                                   from staging_events 
                                   where page = 'NextSong'
                                   limit 1000
                                """, cluster_connect)

    # Convert timestamp column to datetime
    t = pd.to_datetime(song_plays_df.ts, unit='ms')
    
    # Remove duplicate records
    t = t.drop_duplicates()
    
    # Create time data frame
    time_data = ([x.timestamp(),
                  x.hour,
                  x.day,
                  x.week,
                  x.month,
                  x.year,
                  x.weekday()]
                 for x in t)
    column_labels = (['timestamp',
                      'hour',
                      'day',
                      'week',
                      'month',
                      'year',
                      'weekday'])
    time_df = pd.DataFrame(time_data, columns=column_labels)  
#     print(time_df.head())
    
    # Create songplay data frame
    songplay_df = pd.read_sql("""select distinct
                                    events.ts / 1000 as ts,
                                    events.userid,
                                    events.level,
                                    songs.song_id,
                                    songs.artist_id,
                                    events.sessionid,
                                    events.location,
                                    events.useragent
                                  from staging_events as events
                                  left join staging_songs as songs
                                      on events.song = songs.title
                                          and events.artist = songs.artist_name
                                          and events.length = songs.duration
                                  where page = 'NextSong'
                                  limit 1000
                                """, cluster_connect)
    
#     print(songplay_df.head())
    
    # Place data frames into a list and return list
    data_frames = [songplay_df,
                   user_df,
                   song_df,
                   artist_df,
                   time_df]
    
    return data_frames
        
        
def insert_tables(cur, conn, config):
    """
    Insert data stored in data frames from create_data_frames function into \ 
    tables by executing the `insert_table_queries` list from sql_queries.py
    
    INPUTS:
    * cur - the cursor available
    * conn - database connection
    * config - parameters to Redshift cluster
    """
    # Source: https://stackoverflow.com/questions/1663807/how-to-iterate-through-two-lists-in-parallel
    for query, df in list(zip(insert_table_queries, 
                              create_data_frames(cur, conn, config))):
        print(query)
        for i, row in df.iterrows():
            cur.execute(query, list(row))
            conn.commit()


def main():
    """
    Read in parameters needed for Redshift cluster
    Connect to Redshift cluster and gets cursor to it
    Loads all staging tables by calling the load_staging_tables function
    Insert data into tables by calling the insert_tables function, which /
    utilizes the data frames created in the create_data_frames function
    Finally, closes the connection
    """
    
    # Read in parameters needed for Redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Connect to Redshift cluster and gets cursor to it
    conn = psycopg2.connect("""host={} 
                               dbname={} 
                               user={} 
                               password={} 
                               port={}""".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Loads all staging tables by calling the load_staging_tables function
    load_staging_tables(cur, conn)
    
    # Insert data into tables by calling the insert_tables function
    insert_tables(cur, conn, config)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
    