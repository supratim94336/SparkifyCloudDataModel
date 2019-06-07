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
    firstName VARCHAR(50), 
    gender VARCHAR(10), 
    iteminSession INTEGER, 
    lastName VARCHAR(50), 
    length NUMERIC, 
    level VARCHAR(10), 
    location VARCHAR(100), 
    method VARCHAR(10),
    page VARCHAR(50), 
    registration NUMERIC, 
    sessionId INTEGER, 
    song VARCHAR(50),
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR(100),
    userId INTEGER);    
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

# facts ----------------------------------------------------------------
songplay_table_create = """
 CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL REFERENCES time(start_time) sortkey, 
    user_id VARCHAR(50) NOT NULL REFERENCES users(user_id), 
    level VARCHAR(10) NOT NULL, 
    song_id VARCHAR(50) NOT NULL REFERENCES songs(song_id) distkey, 
    artist_id VARCHAR(50) NOT NULL REFERENCES artists(artist_id), 
    session_id INTEGER NOT NULL, 
    location VARCHAR(100) NOT NULL, 
    user_agent VARCHAR(50) NOT NULL);
"""

# dimensions -----------------------------------------------------------
user_table_create = """
 CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY sortkey, 
    first_name VARCHAR(50), 
    last_name VARCHAR(50), 
    gender VARCHAR(10), 
    level VARCHAR(10) NOT NULL)
    diststyle ALL;
"""

song_table_create = """
 CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(50) PRIMARY KEY distkey, 
    title VARCHAR(100) NOT NULL, 
    artist_id VARCHAR(50) NOT NULL, 
    year INTEGER NOT NULL,
    duration NUMERIC NOT NULL);
"""

artist_table_create = """
 CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY sortkey, 
    name VARCHAR(100) NOT NULL, 
    location VARCHAR NOT NULL, 
    latitude NUMERIC NOT NULL, 
    longitude NUMERIC NOT NULL)
    diststyle ALL;
"""

time_table_create = """
 CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP UNIQUE NOT NULL sortkey, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    week_day VARCHAR)
    diststyle ALL;
"""

# INSERT RECORDS
# ----------------------------------------------------------------------
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,
 session_id, location, user_agent) 
 SELECT DISTINCT lgs.ts, 
                 lsg.userId, 
                 nvl(lgs.level, 'empty'), 
                 ssg.song_id, 
                 lsg.artistId,
                 lsg.sessionId, 
                 nvl(lgs.location, 'empty'), 
                 nvl(lgs.userAgent, 'empty')
 FROM log_staging lgs
 INNER JOIN song_staging ssg ON lgs.song = ssg.title
 WHERE lgs.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
  SELECT DISTINCT lgs.userId, 
                  nvl(lgs.firstName, 'empty'), 
                  nvl(lgs.lastName, 'empty'),  
                  nvl(lgs.gender, 'empty'),  
                  nvl(lgs.level, 'empty'), 
  FROM log_staging lgs
  WHERE lgs.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
 SELECT DISTINCT ssg.song_id, 
                 ssg.title, 
                 ssg.artist_id, 
                 ssg.year, 
                 nvl(ssg.duration, 0.0)
  FROM song_staging ssg
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
 SELECT DISTINCT ssg.artist_id, 
                 ssg.artist_name, 
                 nvl(ssg.artist_location, 'empty'), 
                 nvl(ssg.artist_latitude, 0.0), 
                 nvl(ssg.artist_longitude, 0.0)
 FROM song_staging ssg
 WHERE ssg.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
 SELECT DISTINCT se.ts, 
                 DATE_PART(hour, se.ts) :: INTEGER, 
                 DATE_PART(day, se.ts) :: INTEGER, 
                 DATE_PART(week, se.ts) :: INTEGER,
                 DATE_PART(month, se.ts) :: INTEGER,
                 DATE_PART(year, se.ts) :: INTEGER,
                 DATE_PART(dow, se.ts) :: INTEGER
 FROM log_staging lsg
 WHERE lsg.page = 'NextSong';
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

