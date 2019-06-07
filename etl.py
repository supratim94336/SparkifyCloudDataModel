from config import *
import psycopg2
import argparse


def process_song_file():
    """
    i/p: cursor, filepath
    returns: None
    This function takes a json file (song metadata) as input and
    extracts the necessary attributes needed for the songs and the
    artists table, transforms it and uploads it onto the relational
    database
    """
    return None


def process_log_file():
    """
    i/p: cursor, filepath
    returns: None
    This function takes a json file (user activity log file) and
    extracts the necessary attributes needed for the time, users, and
    user activity table, transforms it and loads it on the relational
    database
    """
    return None


def process_data_staging(cur, iam_role):
    """
    i/p: cursor, connection, filepath and ETL function
    returns: None
    This is a helper function for extracting, transforming and loading
    data onto the relational database
    """
    copy_song_command = """
                        copy {}
                        from '{}' 
                        credentials '{}'
                        emptyasnull
                        blanksasnull
                        json 'auto'
                        timeformat 'auto';
                        """.format(DWH_LOG_STAGING_TABLE, S3_BUCKET_LOG_JSON_PATH,
                                   iam_role)
    cur.execute(copy_song_command)

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
    process_data_staging(cur, iam_role)
    return None


if __name__ == "__main__":
    main()
