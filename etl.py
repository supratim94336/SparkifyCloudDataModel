import boto3
import pandas as pd
from sql_queries import *


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


def process_log_file(cur, filepath):
    """
    i/p: cursor, filepath
    returns: None
    This function takes a json file (user activity log file) and
    extracts the necessary attributes needed for the time, users, and
    user activity table, transforms it and loads it on the relational
    database
    """
    return None


def process_data(cur, conn, filepath, func):
    """
    i/p: cursor, connection, filepath and ETL function
    returns: None
    This is a helper function for extracting, transforming and loading
    data onto the relational database
    """
    return None


def main():
    return None


if __name__ == "__main__":
    main()
