import aws_utils
import argparse


parser = argparse.ArgumentParser(description='start/stop')
args = parser.parse_args()
parser.add_argument('action', type=str,
                    help='type an action')
action = args.action

if action == "create":
    roleArn = aws_utils.create_iam_role()
    DWH_ENDPOINT = aws_utils.create_redshift_cluster(roleArn)
    print('DWH_ROLE_ARN={}'.format(roleArn))
    print('DWH_ENDPOINT={}'.format(DWH_ENDPOINT))
elif action == "destroy":
    print('destroying the cluster')

