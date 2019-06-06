from sql_queries import create_table_queries, drop_table_queries
from config import *
import psycopg2
import argparse


def drop_tables(cur, conn):
    """
    This function drops all the tables in the database
    :param cur:
    :param conn:
    :return:
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    return None


def create_tables(cur, conn):
    """
    This function creates all the tables in the database
    :param cur:
    :param conn:
    :return:
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    return None


def main():
    parser = argparse.ArgumentParser(description='Redshift host')
    args = parser.parse_args()
    parser.add_argument('host', type=str, help='type an action')
    DWH_ENDPOINT = args.host

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
    cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(DWH_SCHEMA))
    cur.commit()
    cur.execute("SET search_path to {}".format(DWH_SCHEMA))
    cur.commit()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
