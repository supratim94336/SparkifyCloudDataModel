import boto3
from sql_queries import create_table_queries, drop_table_queries
import os
from config import *
import utils


def create_database():
    """
    This function create databases test and production databases
    :return:
    """
    return None


def drop_tables():
    """
    This function drops all the tables in the database
    :param cur:
    :param conn:
    :return:
    """
    return None


def create_tables():
    """
    This function creates all the tables in the database
    :param cur:
    :param conn:
    :return:
    """
    return None


def main():

    roleArn = utils.create_iam_role()
    DWH_ENDPOINT = utils.create_redshift_cluster(roleArn)

    # create postgres connection
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(
                    DWH_DB_USER,
                    DWH_DB_PASSWORD,
                    DWH_ENDPOINT,
                    DWH_PORT,
                    DWH_DB
    )
    return None


if __name__ == "__main__":
    main()
