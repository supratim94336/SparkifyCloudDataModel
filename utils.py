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
                                              {'Action': 'sts.AssumeRole',
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


