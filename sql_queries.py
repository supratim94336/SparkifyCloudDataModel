# DROP TABLES
# ----------------------------------------------------------------------
log_staging_table_drop = "DROP TABLE IF EXISTS log_staging CASCADE"
song_staging_table_drop = "DROP TABLE IF EXISTS song_staging CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# ----------------------------------------------------------------------
log_staging_table_create = """
 CREATE TABLE IF NOT EXISTS log_staging (
    artist VARCHAR(50), 
    auth VARCHAR(50), 
    firstname VARCHAR(50), 
    gender VARCHAR(10), 
    iteminsession INTEGER, 
    lastname VARCHAR(50), 
    length NUMERIC, 
    level VARCHAR(10), 
    location VARCHAR(100), 
    method VARCHAR(10),
    page VARCHAR(50), 
    registration NUMERIC, 
    sessionid INTEGER, 
    song VARCHAR(50),
    status INTEGER,
    ts TIMESTAMP,
    useragent VARCHAR(100),
    userid INTEGER);    
"""

song_staging_table_create = """
 CREATE TABLE IF NOT EXISTS song_staging (
    num_songs INTEGER, 
    artist_id VARCHAR(50), 
    artist_latitude NUMERIC, 
    artist_longitude NUMERIC, 
    artist_location VARCHAR(100), 
    artist_name VARCHAR(100), 
    song_id NUMERIC, 
    title VARCHAR(50), 
    duration NUMERIC, 
    year INTEGER);    
"""

songplay_table_create = """
 CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL, 
    user_id VARCHAR(50) NOT NULL, 
    level VARCHAR(10) NOT NULL, 
    song_id VARCHAR(50), 
    artist_id VARCHAR(50), 
    session_id INTEGER NOT NULL, 
    location VARCHAR(100), 
    user_agent VARCHAR(50),
 CONSTRAINT songplayuser 
 UNIQUE(start_time, user_id, level, session_id));
"""

user_table_create = """
 CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY, 
    first_name VARCHAR(50), 
    last_name VARCHAR(50), 
    gender VARCHAR(10), 
    level VARCHAR(10) NOT NULL);
"""

song_table_create = """
 CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(50) PRIMARY KEY, 
    title VARCHAR(100) NOT NULL, 
    artist_id VARCHAR(50) NOT NULL, 
    year INTEGER, 
    duration NUMERIC NOT NULL);
"""

artist_table_create = """
 CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY, 
    name VARCHAR(100) NOT NULL, 
    location VARCHAR, 
    latitude NUMERIC, 
    longitude NUMERIC);
"""

time_table_create = """
 CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP UNIQUE NOT NULL, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    week_day VARCHAR);
"""

# INSERT RECORDS
# ----------------------------------------------------------------------
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,
 session_id, location, user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
 ON CONFLICT ON CONSTRAINT songplayuser
 DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
 VALUES (%s,%s,%s,%s,%s)
 ON CONFLICT (user_id) 
 DO
 UPDATE
   SET level = EXCLUDED.level, first_name = EXCLUDED.first_name,
   last_name = EXCLUDED.last_name, gender = EXCLUDED.gender;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
 VALUES (%s,%s,%s,%s,%s)
 ON CONFLICT (song_id) 
 DO
 NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
 VALUES (%s,%s,%s,%s,%s)
 ON CONFLICT (artist_id) 
 DO
 NOTHING;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, week_day) 
 VALUES (%s,%s,%s,%s,%s,%s,%s)
 ON CONFLICT (start_time)
 DO
 NOTHING;
""")

# FIND SONGS
# you'll need to get the song ID and artist ID by querying the songs
# and artists tables to find matches based on song title, artist name,
# and song duration time
song_select = ("""
SELECT s.song_id, s.artist_id FROM songs s
 JOIN artists a ON s.artist_id=a.artist_id
 WHERE s.title = %s AND a.name=%s AND s.duration=%s;
""")

# QUERY LISTS

create_table_queries = [log_staging_table_create, song_staging_table_create, songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [log_staging_table_drop, song_staging_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop,
                      time_table_drop]

