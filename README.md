<img align="right" src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/93/Amazon_Web_Services_Logo.svg/512px-Amazon_Web_Services_Logo.svg.png" width=108>


## Sparkify Cloud Data Model with AWS
Udacity project on Cloud Data Warehousing with Amazon Redshift and AWS

### Data Modelling with Amazon Redshift
- This data-ware-housing project is based on Amazon Web Services (aws)
- The reason behind choosing a cloud-data-ware-housing model is to better
manage the infrastructure and provide more flexibility in terms of money and resources
- The infrastructure as code (IAC) model used in this project is very easy and requires
very few manual efforts to set up
- Currently, we have a Star Schema architecture with Artists, Users, 
Time, and Songs are used as dimension tables and Songplays is used as 
Fact table because Songplays will have events that happened and 
different dimensions will give us perspective from different angles
- The distribution and sorting of different tables across the nodes help
the query-computing very un-intense and requires very less span of time
- The code is written in Python SDK of aws (boto3), just by using the
  code, and following the instructions one can set up a
  mini cloud data warehousing architecture very easily and in minutes
  
### The Star Schema that we are going to design here

![alt text](img/schema.png)


### Getting Started
Download the project:
You can download it as zip and unpack the files or you can clone this
repository

#### Prerequisites
- One should have anaconda/miniconda installed in his/her machine
- Signup for an aws account
- Create an IAM User (https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html)
   - The user created by you in AWS should have read access to S3, Admin
   access to Redshift, Admin access to IAM
- Get your secret key and access key https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html
   - Download as CSV or save somewhere the generated key and the secret
     for dwh.cfg file
- Update dwh.cfg with the key and the secret
- (optional) One should have an aws account with a redshift cluster
running and a
  user role created
   - Create Redshift Cluster https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html)
   - Create IAM role https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create.html

#### Installing and starting

##### Installing Python Dependencies
You need to install this python dependencies
In Terminal/CommandPrompt:
Step 1: At first open: anaconda prompt/cmder/Command prompt/terminal and
 go inside the cloned directory

Step 2: Now, with the first step already completed,  
without anaconda:
```
$ python3 -m venv virtual-env-name
$ source virtual-env-name/bin/activate
$ pip install -r requirements.txt
```
with anaconda
```
$ conda env create -f env.yml
$ source activate <conda-env-name>
```
or
```
conda create -y -n <conda-env-name> python==3.6
conda install -f -y -q -n <conda-env-name> -c conda-forge --file requirements.txt
[source activate/ conda activate] <conda-env-name>
```

Step 3: Change file name of dwh_dummy_file.cfg to dwh.cfg and insert the
key and secret from previous steps

##### Executing the creation and extraction scripts
**Note: Step 0 and Step 3 are optional if you already created your Redshift
cluster and also created an iam role with read access to
Amazon S3 manually*  

**Step 0**: Run the below code to create your Redshift database and a
iam_role in aws having read access to Amazon S3 and permissions 
attached to the created cluster
```bash
$ python aws_operate.py --action start
```
copy the DWH_ENDPOINT for <cluster_endpoint_address> and DWH_ROLE_ARN 
for <iam_role> from the print statements 
**Step 1**: Run the below code to create tables in your Redshift database
in aws
```bash
$ python create_table.py --host <cluster_endpoint_address>
```
**Step 2**: Run the below code to populate your Redshift database
in aws
```bash
$ python etl.py host --host <cluster_endpoint_address> --credentials <iam_role>
```
**Step 3**: Run the below code to destroy your Redshift database and
detach iam_role from the cluster in aws
```bash
$ python aws_operate.py --action stop
```

### About the data
Song Data is collected from the Million Song Dataset   
Check: http://millionsongdataset.com/

Log Data is generated artificially  
Check: https://github.com/Interana/eventsim

### Example Queries
For the most popular songs over the time (considering your schema name 
is sparkify)
```
SELECT s.title, count(*) as count
FROM sparkify.songplays sp
INNER JOIN sparkify.songs s ON s.song_id = sp.song_id
GROUP BY s.title
ORDER BY count DESC, s.title ASC
```
For the most popular artists and their songs over the time (considering 
your schema name is sparkify)
```
SELECT ar.name, s.title, count(*) as count
FROM sparkify.songplays sp
INNER JOIN sparkify.songs s ON s.song_id = sp.song_id
INNER JOIN sparkify.artists ar ON ar.artist_id = sp.artist_id
GROUP BY ar.name, s.title
ORDER BY count DESC, ar.name, s.title ASC;
```
### View and Analyze
- The Tester.ipynb notebook is there to test your data with different
queries (already some pre-written there)
- The connection to the cluster is already written, you just have to
    - fill up your cluster end point and iam role
    - execute block by block to run the queries

### Warning
<span style="color:red">
    Do not share your AWS KEY and AWS SECRET in github! You will get 
    calls from DC right away! And trust me on this
</span>
 
### Authors
* **Supratim Das** - *Initial work*
