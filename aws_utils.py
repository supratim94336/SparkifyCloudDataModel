import boto3
from config import *
import json
from botocore.exceptions import ClientError
import utils
from smart_open import open


def create_iam_role():
    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                       )
    print("1.1 creating role")
    try:
        iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift to call AWS Services.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                  'Effect': 'Allow',
                  'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
            )

    except ClientError as e:
        print(f'ERROR: {e}')

    print("1.2 Attaching Policy")
    try:
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")\
                        ['ResponseMetadata']['HTTPStatusCode']
    except ClientError as e:
        print(f'ERROR: {e}')

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    return roleArn


def create_redshift_cluster(roleArn):
    print("1.1 Client is created ...")
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    try:
        print("1.2 Cluster config is being created ...")
        redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn])
    except ClientError as e:
        print(f'ERROR: {e}')

    print("1.3 Cluster is being created ...")
    while redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)\
            ['Clusters'][0]['ClusterStatus'] != 'available':
        utils.animate()

    print("\r1.4 Cluster is created successfully ...")
    return redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)\
    ['Clusters'][0]['Endpoint']['Address']


def delete_redshift_cluster():
    print("1.1 Client is created ...")
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    print("1.2 Cluster is identified ...")
    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True)
    except ClientError as e:
        print(f'ERROR: {e}')

    try:
        print("1.3 Cluster is being deleted ...")
        while redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)\
                ['Clusters'][0]['ClusterStatus'] == 'deleting':
            utils.animate()
    except:
        print("\r1.4 Cluster is deleted successfully ...")
    return None


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


def list_bucket(bucket_name, prefix):
    """

    :param bucket_name: Your S3 BucketName
    :param key: Original Name and type of the file you want to upload
                into s3
    :param output_name: Output file name(The name you want to give to
                        the file after we upload to s3)
    :return:
    """
    files = []
    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        files.append(obj)
    return files


def s3_read(s3_path):
    """
    Read a file from an S3 source.

    Parameters
    ----------
    source : str
        Path starting with s3://, e.g. 's3://bucket-name/key/foo.bar'
    profile_name : str, optional
        AWS profile

    Returns
    -------
    content : bytes

    botocore.exceptions.NoCredentialsError
        Botocore is not able to find your credentials. Either specify
        profile_name or add the environment variables AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN.
        See https://boto3.readthedocs.io/en/latest/guide/configuration.html
    """
    for line in open(s3_path, 'rb', encoding='utf-8'):
        print(line.decode('utf8'))
