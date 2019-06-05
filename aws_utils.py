import boto3
from config import *
import json
from botocore.exceptions import ClientError
import time


def create_iam_role():
    iam = boto3.client('iam',
                       region_name="us-west-2",
                       aws_access_key=KEY,
                       aws_secret_access_key=SECRET
                       )
    try:
        response = iam.create_role(Path='/',
                                  RoleName=DWH_IAM_ROLE_NAME,
                                  Description="Allows Redshift clusters to "
                                              "call AWS services on your "
                                              "behalf.",
                                  AssumeRolePolicyDocument=json.dumps(
                                      {
                                          'Statement': [
                                              {'Action': [
                                                  "s3:GetObject",
                                                  "s3:ListBucket"
                                              ],
                                               'Effect': 'Allow',
                                               'Principal': {'Service': 'redshift.amazonaws.com'}}
                                          ],
                                          'Version': '2012-10-17'
                                      }
                                  )
                                 )
    except ClientError as e:
        print(f'ERROR: {e}')
    else:
        return response['Role']['Arn']


def create_redshift_cluster(roleArn):
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key=KEY,
                            aws_secret_access_key=SECRET
                            )
    try:
        response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=DWH_NUM_NODES,
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUserName=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn]
        )
    except ClientError as e:
        print(f'ERROR: {e}')
    else:
        while response['Cluster']['ClusterStatus'] != 'available':
            time.sleep(10)
        return response['Cluster']['Endpoint']['Address']


def create_bucket(bucket_name):
    """ Create an Amazon S3 bucket

    :param bucket_name: Unique string name
    :return: True if bucket is created, else False
    """
    s3 = boto3.client('s3')
    try:
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(f'ERROR: {e}')
        return False
    return True


def upload_bucket(bucket_name, key, output_name):
    """

    :param bucket_name: Your S3 BucketName
    :param key: Original Name and type of the file you want to upload
                into s3
    :param output_name: Output file name(The name you want to give to
                        the file after we upload to s3)
    :return:
    """
    s3 = boto3.client('s3')
    s3.upload_file(key, bucket_name, output_name)
