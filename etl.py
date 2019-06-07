from config import *
import psycopg2
import argparse
import pandas.io.sql as psql
from sql_queries import *
import pandas as pd


def process_song_file(conn, cur, staging_table):
    """
    i/p: cursor, filepath
    returns: None
    This function takes a json file (song metadata) as input and
    extracts the necessary attributes needed for the songs and the
    artists table, transforms it and uploads it onto the relational
    database
    """
    sql_query = "SELECT * FROM {}".format(staging_table)
    df = psql.read_sql(sql_query, conn)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year',
                    'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    conn.commit()

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location',
                      'artist_latitude',
                      'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    conn.commit()
    return None


def process_log_file(conn, cur, staging_table):
    """
    i/p: cursor, filepath
    returns: None
    This function takes a json file (user activity log file) and
    extracts the necessary attributes needed for the time, users, and
    user activity table, transforms it and loads it on the relational
    database
    """
    sql_query = "SELECT * FROM {}".format(staging_table)
    df = psql.read_sql(sql_query, conn)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month,
                 t.dt.year, t.dt.weekday_name)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month',
                     'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df[['timestamp', 'hour', 'day', 'week', 'month',
                           'year', 'weekday']].iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid,
                         artistid, row.sessionId, row.location,
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
    return None


def process_data_staging(cur, conn, iam_role):
    """
    i/p: cursor, connection, filepath and ETL function
    returns: None
    This is a helper function for extracting, transforming and loading
    data onto the relational database
    """
    cur.execute("SET search_path to {}".format(DWH_SCHEMA))
    conn.commit()
    copy_song_command = """
                        copy {}
                        from '{}' 
                        credentials '{}'
                        emptyasnull
                        blanksasnull
                        format as json {}
                        timeformat 'auto';
                        """.format(DWH_LOG_STAGING_TABLE, S3_BUCKET_LOG_JSON_PATH,
                                   iam_role, LOG_JSON_FORMAT)
    cur.execute(copy_song_command)

    cur.execute("SET search_path to {}".format(DWH_SCHEMA))
    conn.commit()
    copy_song_command = """
                        copy {}
                        from '{}' 
                        credentials '{}'
                        emptyasnull
                        blanksasnull
                        json 'auto'
                        timeformat 'auto';
                            """.format(DWH_SONG_STAGING_TABLE, S3_BUCKET_SONG_JSON_PATH,
                                       iam_role)
    cur.execute(copy_song_command)
    return None


def main():
    parser = argparse.ArgumentParser(description='Configurations')
    args = parser.parse_args()
    parser.add_argument('host', type=str, help='redshift host')
    parser.add_argument('credentials', type=str, help='userId')

    DWH_ENDPOINT = args.host
    iam_role = args.user

    # create postgres connection
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(
        DWH_DB_USER,
        DWH_DB_PASSWORD,
        DWH_ENDPOINT,
        DWH_PORT,
        DWH_DB
    )
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    process_data_staging(cur, conn, iam_role)
    process_song_file(conn, cur, staging_table=DWH_SONG_STAGING_TABLE)
    process_log_file(conn, cur, staging_table=DWH_LOG_STAGING_TABLE)
    return None


if __name__ == "__main__":
    main()
