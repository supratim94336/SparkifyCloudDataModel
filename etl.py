import boto3
import pandas as pd
from sql_queries import *


def process_song_file(table_name, s3_path, iam_role, json_path):
    """
    i/p: cursor, filepath
    returns: None
    This function takes a json file (song metadata) as input and
    extracts the necessary attributes needed for the songs and the
    artists table, transforms it and uploads it onto the relational
    database
    copy category
    from 's3://mybucket/category_object_paths.json'
    iam_role 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
    json 's3://mybucket/category_jsonpath.json';
    category_jsonpath.json looks like
    {
        "jsonpaths": [
            "$['start_time']",
            "$['user_id']",
            "$['level']",
            "$['song_id']",
            "$['artist_id']",
            "$['session_id']",
            "$['location']",
            "$['user_agent']"
        ]
    }
    """
    copy_song_command = """
                        copy {} 
                        from '{}' 
                        credentials 'aws_iam_role={}'
                        json '{}'
                        region 'us-west-2';
                        """.format(table_name, s3_path, iam_role,
                                   json_path)
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
    bucketName = "Your S3 BucketName"
    Key = "Original Name and type of the file you want to upload into s3"
    outPutname = "Output file name(The name you want to give to the file after we upload to s3)"

    s3 = boto3.client('s3')
    s3.upload_file(Key, bucketName, outPutname)
    return None


def main():
    return None


if __name__ == "__main__":
    main()
