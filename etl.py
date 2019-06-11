from config import *
import psycopg2
import argparse
from sql_queries import copy_log_command, copy_song_command, \
    insert_table_queries


def process_data_staging(cur, conn, iam_role):
    """
    i/p: cursor, connection, filepath and ETL function
    returns: None
    This is a helper function for extracting, transforming and loading
    data onto the relational database
    """
    # copy logs of users
    copy_log = copy_log_command.format(DWH_SCHEMA,
                                       DWH_LOG_STAGING_TABLE,
                                       S3_BUCKET_LOG_JSON_PATH,
                                       iam_role,
                                       LOG_JSON_FORMAT)

    cur.execute(copy_log)
    conn.commit()

    # copy songs played by users
    copy_song = copy_song_command.format(DWH_SCHEMA,
                                         DWH_SONG_STAGING_TABLE,
                                         S3_BUCKET_SONG_JSON_PATH,
                                         iam_role)
    cur.execute(copy_song)
    conn.commit()
    return None


def insert_data_into_tables(cur, conn):
    """
    This function creates all the tables in the database
    :param cur:
    :param conn:
    :return:
    """
    cur.execute("SET search_path to {}".format(DWH_SCHEMA))
    conn.commit()
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    return None


def main():
    # get command line arguments
    parser = argparse.ArgumentParser(description='Configurations')
    parser.add_argument('--host', type=str, help='redshift host')
    parser.add_argument('--credentials', type=str, help='userId')
    args = parser.parse_args()
    DWH_ENDPOINT = args.host
    iam_role = args.credentials

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
    # extract files from s3 server and insert data in staging tables
    process_data_staging(cur, conn, iam_role)
    # insert data into facts and dimensions from staging tables
    insert_data_into_tables(cur, conn)
    conn.close()
    return None


if __name__ == "__main__":
    main()
