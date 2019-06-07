import aws_utils
import argparse


parser = argparse.ArgumentParser(description='start/stop')
args = parser.parse_args()
parser.add_argument('action', type=str,
                    help='type an action')
action = args.action

if action == "start":
    roleArn = aws_utils.create_iam_role()
    DWH_ENDPOINT = aws_utils.create_redshift_cluster(roleArn)
    print('DWH_ROLE_ARN={}'.format(roleArn))
    print('DWH_ENDPOINT={}'.format(DWH_ENDPOINT))
elif action == "stop":
    print('destroying the cluster')
    aws_utils.delete_redshift_cluster()

