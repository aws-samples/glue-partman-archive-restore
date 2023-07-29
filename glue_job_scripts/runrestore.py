# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import boto3
from awsglue.utils import getResolvedOptions
import psycopg2
import json

args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME','WORKFLOW_RUN_ID','db_secret_arn'])
# Get the connector name from workflow parameters
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']
workflow_params = boto3.client("glue").get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]
bucket_name = workflow_params['bucket_name']
restore_date = workflow_params['restore_date']
parent_table = workflow_params['parent_table']
schema = workflow_params['schema']
region = workflow_params['region']

print ("bucket_name :", bucket_name, "restore_date :", restore_date, "schema :", schema, "parent_table :", parent_table)
# Connect to the database
# Get the database connection information from the secret stored in secrets manager
secrets_manager_client = boto3.client('secretsmanager')
secret_value = secrets_manager_client.get_secret_value(SecretId=args['db_secret_arn'])
database_secrets = json.loads(secret_value['SecretString'])
connection = psycopg2.connect(database=database_secrets['dbname'], user = database_secrets['username'], 
                        password = database_secrets['password'], host = database_secrets['host'], 
                        port = database_secrets['port'])
cursor = connection.cursor()
print ("Got connection and cursor")
# Re-create the partition table using partman for the given restore date
sql = "SELECT partman.create_partition_time( p_parent_table => '"+schema+"."+parent_table+"', p_partition_times => ARRAY['"+restore_date+"']::timestamptz[]);"
cursor.execute(sql)
result = cursor.fetchall()
print ("Result :", result)      # true a partiton table was created, false it was not created (because it was already there)
# For the given date, discover the schema and name of partition table. We will need this to reference the backup in S3
sql = "SELECT partman.show_partition_name (p_parent_table => '"+schema+"."+parent_table+"', p_value => '"+restore_date+"');"
print("Show partition name sql: ", sql)
cursor.execute(sql)
print ("Executed")
result = cursor.fetchone()
print("result :", result)
key = result[0]
tokens = key.split("(")
item = tokens[1]
tokens = item.split(",")
schema = tokens[0]
table = tokens[1]
print ("schema :", schema)
print ("table :", table)
# Write the archive data from S3 to the parent table. This will go to the partition we just recreated
# The path to the backup was determined by the schema and partition table name
bucket_path = database_secrets['dbname']+"/"+schema+"/"+parent_table+"/"+table+"/data"
print ("Bucket path :", bucket_path)
sql = "SELECT * FROM aws_s3.table_import_from_s3('"+schema+"."+parent_table+"', '',  '(format text)', aws_commons.create_s3_uri( '"+bucket_name+"', '"+bucket_path+"', '"+region+"'))"
print("Restore sql: ", sql)
cursor.execute(sql)
result = cursor.fetchall()
print ("Restore from S3 result :", result)      # true a partiton table was created, false it was not created (because it was already there)
connection.commit()
print ("Restore complete")
cursor.close()
connection.close()