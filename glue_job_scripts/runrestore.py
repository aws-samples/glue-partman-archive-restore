# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import psycopg2
from urllib.parse import urlparse
from subprocess import PIPE,Popen

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','WORKFLOW_NAME','WORKFLOW_RUN_ID'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
glue_client = boto3.client("glue")

#
# Get the connector name from workflow parameters
#
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']
workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]
connection = workflow_params['connection']
bucket_name = workflow_params['bucket_name']
restore_date = workflow_params['restore_date']
parent_table = workflow_params['parent_table']
region = workflow_params['region']

print ("connection :",connection, "bucket_name :", bucket_name, "restore_date :", restore_date, "parent_table :", parent_table)
#
# Get the JDBC Configuration from the connection and parse out the 
# host, port, database name, dbusername and password
#
jdbc_conf = glueContext.extract_jdbc_conf(connection_name=connection)

user = jdbc_conf['user']
password = jdbc_conf['password']
fullUrl = jdbc_conf['fullUrl']
url = fullUrl.replace("jdbc:postgresql:", "http:")
parsedurl = urlparse(url)
port = parsedurl.port
hostname = parsedurl.hostname
db = parsedurl.path
database = db.replace("/", "", 1)

#
# Connect to the database
#
connection = psycopg2.connect(database=database, user = user, password = password, host = hostname, port = port)
#connection.autocommit = True
cursor = connection.cursor()
print ("Got connection and cursor")


#
# Re-create the partition table using partman for the given restore date
#
sql = "SELECT partman.create_partition_time( p_parent_table => '"+parent_table+"', p_partition_times => ARRAY['"+restore_date+"']::timestamptz[]);"
cursor.execute(sql)
result = cursor.fetchall();
print ("Result :", result)      # true a partiton table was created, false it was not created (because it was already there)
#
# For the given date, discover the schema and name of partition table. We will need this to reference the backup in S3
#
sql = "SELECT partman.show_partition_name (p_parent_table => '"+parent_table+"', p_value => '"+restore_date+"');"
print("Show partition name sql: ", sql)
cursor.execute(sql)
print ("Executed")
result = cursor.fetchone();
print("result :", result)
key = result[0]
tokens = key.split("(")
item = tokens[1]
tokens = item.split(",")
schema = tokens[0]
table = tokens[1]
print ("schema :", schema)
print ("table :", table)
#
# Write the archive data from S3 to the parent table. This will go to the partition we just recreated
# The path to the backup was determined by the schema and partition table name
#
bucket_path = database+"/"+schema+"/"+table+"/data"
print ("Bucket path :", bucket_path)
sql = "SELECT * FROM aws_s3.table_import_from_s3('"+parent_table+"', '',  '(format text)', aws_commons.create_s3_uri( '"+bucket_name+"', '"+bucket_path+"', '"+region+"'))"
    
print("Restore sql: ", sql)
cursor.execute(sql)
result = cursor.fetchall();
print ("Restore from S3 result :", result)      # true a partiton table was created, false it was not created (because it was already there)

connection.commit()
print ("Restore complete")
cursor.close()
connection.close()

job.commit()

