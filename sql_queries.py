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
    artist TEXT, 
    auth TEXT, 
    firstName TEXT, 
    gender TEXT, 
    ItemInSession INT, 
    lastName TEXT, 
    length FLOAT8, 
    level TEXT, 
    location TEXT, 
    method TEXT, 
    page TEXT, 
    registration TEXT, 
    sessionId INT, 
    song TEXT, 
    status INT, 
    ts timestamp, 
    userAgent TEXT, 
    userId INT);    
"""

song_staging_table_create = """
 CREATE TABLE IF NOT EXISTS song_staging (
    song_id TEXT PRIMARY KEY, 
    artist_id TEXT, 
    artist_latitude FLOAT8, 
    artist_location TEXT, 
    artist_longitude FLOAT8, 
    artist_name TEXT, 
    duration FLOAT8, 
    num_songs INT, 
    title TEXT, 
    year INT);    
"""

# facts ----------------------------------------------------------------
songplay_table_create = """
 CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY PRIMARY KEY, 
    start_time timestamp NOT NULL REFERENCES time(start_time) sortkey, 
    user_id INT NOT NULL REFERENCES users(user_id), 
    level TEXT NOT NULL, 
    song_id TEXT NOT NULL REFERENCES songs(song_id), 
    artist_id TEXT NOT NULL REFERENCES artists(artist_id) distkey, 
    session_id INT NOT NULL, 
    location TEXT NOT NULL, 
    user_agent TEXT NOT NULL);
"""

# dimensions -----------------------------------------------------------
user_table_create = """
 CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY sortkey, 
    first_name TEXT NOT NULL, 
    last_name TEXT NOT NULL, 
    gender TEXT NOT NULL, 
    level TEXT NOT NULL)
    diststyle ALL;
"""

song_table_create = """
 CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY sortkey, 
    title TEXT NOT NULL, 
    artist_id TEXT NOT NULL, 
    year INT NOT NULL, 
    duration NUMERIC NOT NULL)
    diststyle ALL;
"""

artist_table_create = """
 CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY distkey, 
    name TEXT NOT NULL, 
    location TEXT NOT NULL, 
    lattitude FLOAT8 NOT NULL, 
    longitude FLOAT8 NOT NULL)
"""

time_table_create = """
 CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY sortkey, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL)
"""

# INSERT RECORDS
# ----------------------------------------------------------------------
songplay_table_insert = ("""
INSERT INTO sparkify.songplays (start_time, user_id, level, song_id, artist_id,
 session_id, location, user_agent) 
 SELECT DISTINCT lgs.ts, 
                 lgs.userId, 
                 nvl(lgs.level, 'empty'), 
                 ssg.song_id, 
                 ssg.artist_id,
                 lgs.sessionId, 
                 nvl(lgs.location, 'empty'), 
                 nvl(lgs.userAgent, 'empty')
 FROM sparkify.log_staging lgs
 INNER JOIN sparkify.song_staging ssg ON lgs.song = ssg.title
 WHERE lgs.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO sparkify.users (user_id, first_name, last_name, gender, level) 
  SELECT DISTINCT lgs.userId, 
                  nvl(lgs.firstName, 'empty'), 
                  nvl(lgs.lastName, 'empty'),  
                  nvl(lgs.gender, 'empty'),  
                  nvl(lgs.level, 'empty')
  FROM sparkify.log_staging lgs
  WHERE lgs.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO sparkify.songs (song_id, title, artist_id, year, duration) 
 SELECT DISTINCT ssg.song_id, 
                 ssg.title, 
                 ssg.artist_id, 
                 ssg.year, 
                 nvl(ssg.duration, 0.0)
  FROM sparkify.song_staging ssg
""")

artist_table_insert = ("""
INSERT INTO sparkify.artists (artist_id, name, location, latitude, longitude) 
 SELECT DISTINCT ssg.artist_id, 
                 ssg.artist_name, 
                 nvl(ssg.artist_location, 'empty'), 
                 nvl(ssg.artist_latitude, 0.0), 
                 nvl(ssg.artist_longitude, 0.0)
 FROM sparkify.song_staging ssg
 WHERE ssg.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO sparkify.time (start_time, hour, day, week, month, year, week_day)
 SELECT DISTINCT lgs.ts, 
                 DATE_PART(hour, lgs.ts) :: INTEGER, 
                 DATE_PART(day, lgs.ts) :: INTEGER, 
                 DATE_PART(week, lgs.ts) :: INTEGER,
                 DATE_PART(month, lgs.ts) :: INTEGER,
                 DATE_PART(year, lgs.ts) :: INTEGER,
                 DATE_PART(dow, lgs.ts) :: INTEGER
 FROM sparkify.log_staging lgs
 WHERE lgs.page = 'NextSong';
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

create_table_queries = [log_staging_table_create,
                        song_staging_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]
insert_table_queries = [user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert,
                        songplay_table_insert]
drop_table_queries = [log_staging_table_drop,
                      song_staging_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

