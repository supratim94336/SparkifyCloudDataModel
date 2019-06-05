# DROP TABLES
# ----------------------------------------------------------------------
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# ----------------------------------------------------------------------
songplay_table_create = ("""
 CREATE TABLE IF NOT EXISTS songplays (
    songplay_id  IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id varchar(50) NOT NULL, 
    level varchar(10) NOT NULL, 
    song_id varchar(50), 
    artist_id varchar(50), 
    session_id int NOT NULL, 
    location varchar(100), 
    user_agent varchar(50),
 CONSTRAINT songplayuser 
 UNIQUE(start_time, user_id, level, session_id));
""")

user_table_create = ("""
 CREATE TABLE IF NOT EXISTS users (
    user_id varchar(50) PRIMARY KEY, 
    first_name varchar(50), 
    last_name varchar(50), 
    gender varchar(10), 
    level varchar(10) NOT NULL);
""")

song_table_create = ("""
 CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(50) PRIMARY KEY, 
    title varchar(100) NOT NULL, 
    artist_id varchar(50) NOT NULL, 
    year int, 
    duration numeric NOT NULL);
""")

artist_table_create = ("""
 CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(50) PRIMARY KEY, 
    name varchar(100) NOT NULL, 
    location varchar, 
    latitude numeric, 
    longitude numeric);
""")

time_table_create = ("""
 CREATE TABLE IF NOT EXISTS time (
    start_time timestamp UNIQUE NOT NULL, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    week_day varchar);
""")

# INSERT RECORDS

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

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop,
                      time_table_drop]

