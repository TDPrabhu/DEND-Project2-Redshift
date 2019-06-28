import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
""" 
 Drop all the tables 
 
"""

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

""" 
 Create all the tables 
 
"""

staging_events_table_create=  ("""CREATE TABLE staging_events(
                                    artist_name VARCHAR(255),
                                    auth VARCHAR(50),
                                    first_name VARCHAR(255),
                                    gender  VARCHAR(1),
                                    item_in_session	INTEGER,
                                    last_name VARCHAR(255),
                                    song_length	DOUBLE PRECISION, 
                                    level VARCHAR(50),
                                    location VARCHAR(255),	
                                    method VARCHAR(25),
                                    page VARCHAR(35),	
                                    registration VARCHAR(50),	
                                    session_id	BIGINT,
                                    song_title VARCHAR(255),
                                    status INTEGER,  	
                                    ts bigint,
                                    user_agent TEXT,	
                                    user_id VARCHAR(100)
                                   )
                                """)
staging_songs_table_create = ("""CREATE TABLE staging_songs(
                                    song_id VARCHAR(100),
                                    num_songs INTEGER,
                                    artist_id VARCHAR(100),
                                    latitude DOUBLE PRECISION,
                                    longitude DOUBLE PRECISION,
                                    location VARCHAR(255),
                                    name VARCHAR(255),
                                    title VARCHAR(255),
                                    duration DOUBLE PRECISION,
                                    year INTEGER
                                    )
                             """)

user_table_create = (""" CREATE TABLE users
                                (
                                     user_id VARCHAR(255),
                                     firstName VARCHAR(255),
                                     lastName VARCHAR(255),
                                     gender VARCHAR(1),
                                     level VARCHAR(255),
                                     PRIMARY KEY (user_id)
                                 )
                    """)

song_table_create = (""" CREATE TABLE songs
                                         (
                                            song_id VARCHAR(255),
                                            title VARCHAR(255),
                                            artist_id VARCHAR(255),
                                            year INTEGER,
                                            duration DOUBLE PRECISION,
                                            PRIMARY KEY (song_id)
                                          )
                    
                    """)

artist_table_create = (""" CREATE TABLE artists
                                            (
                                                artist_id VARCHAR(255),
                                                name VARCHAR(255),
                                                location VARCHAR(255),
                                                latitude DOUBLE PRECISION,
                                                longitude DOUBLE PRECISION,
                                                PRIMARY KEY (artist_id)
                                               )
                      """)

time_table_create = ("""  CREATE TABLE time
                                        (
                                            start_time TIMESTAMP,
                                            hour INTEGER,
                                            day INTEGER,
                                            week INTEGER,
                                            month INTEGER,
                                            year INTEGER,
                                            weekday varchar,
                                            PRIMARY KEY (start_time)
                                         )
                    """)

songplay_table_create = (""" CREATE TABLE songplays
                            (
                                 songplay_id INT IDENTITY(0,1),
                                 start_time TIMESTAMP REFERENCES time(start_time),
                                 user_id VARCHAR(255) REFERENCES users(user_id),
                                 level VARCHAR(255),
                                 song_id VARCHAR(255) REFERENCES songs(song_id),
                                 artist_id VARCHAR(255) REFERENCES artists(artist_id),
                                 session_id BIGINT,
                                 location VARCHAR(255),
                                 user_agent TEXT,
                                 PRIMARY KEY (songplay_id) 
                             )
                        """)

# STAGING TABLES

""" 
 Load the data from S3 into staging tables  (Redshift)
 
"""

staging_events_copy = ("""    copy staging_events from '{}'
                              credentials 'aws_iam_role={}'
                              region 'us-west-2' 
                              COMPUPDATE OFF STATUPDATE OFF
                              JSON '{}'
                       """).format(config.get('S3','LOG_DATA'),
                                    config.get('IAM_ROLE', 'ARN'),
                                    config.get('S3','LOG_JSONPATH')
                                    )

staging_songs_copy = ("""   copy staging_songs from '{}'
                            credentials 'aws_iam_role={}'
                            region 'us-west-2' 
                            COMPUPDATE OFF STATUPDATE OFF
                            JSON 'auto'
                      """).format(config.get('S3','SONG_DATA'), 
                                  config.get('IAM_ROLE', 'ARN')
                                 )

# FINAL TABLES

""" 
 Insert the data from Staging table into fact and dimnesion table  .
 
"""

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location,user_agent) 
                            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
                                a.user_id, 
                                a.level,
                                b.song_id,
                                b.artist_id,
                                a.session_id,
                                a.location,
                                a.user_agent
                            FROM staging_events a, staging_songs b
                            WHERE a.page = 'NextSong'
                            AND a.song_title = b.title
                            AND a.user_id NOT IN (SELECT  c.user_id FROM songplays c WHERE c.user_id = a.user_id
                                                  AND c.start_time = start_time AND c.session_id = a.session_id 
                                                  )
                            """)

user_table_insert = (""" INSERT INTO users (user_id, firstname, lastname, gender, level)  
                            SELECT DISTINCT 
                                a.user_id,
                                a.first_name,
                                a.last_name,
                                a.gender, 
                                a.level
                            FROM staging_events a
                            WHERE a.page = 'NextSong'
                            AND a.user_id NOT IN (SELECT  user_id FROM users)
                            
                     """)

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT 
                            a.song_id, 
                            a.title,
                            a.artist_id,
                            a.year,
                            a.duration
                        FROM staging_songs a
                        WHERE song_id NOT IN (SELECT song_id FROM songs)
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                        SELECT  DISTINCT 
                                a.artist_id,
                                a.name,
                                a.location,
                                a.latitude,
                                a.longitude
                            FROM staging_songs a
                            WHERE artist_id NOT IN (SELECT artist_id FROM artists)
                        """)

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                         SELECT 
                                start_time, 
                                EXTRACT(hr from start_time) AS hour,
                                EXTRACT(d from start_time) AS day,
                                EXTRACT(w from start_time) AS week,
                                EXTRACT(mon from start_time) AS month,
                                EXTRACT(yr from start_time) AS year, 
                                EXTRACT(weekday from start_time) AS weekday 
                            FROM (
                                    SELECT DISTINCT  TIMESTAMP 'epoch' + a.ts/1000 *INTERVAL '1 second' as start_time 
                                    FROM staging_events a    
                                 )
                            WHERE start_time NOT IN (SELECT start_time FROM time)
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create, song_table_create, artist_table_create,time_table_create,songplay_table_create,]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
