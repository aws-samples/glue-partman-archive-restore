# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import boto3
import json
from awsglue.utils import getResolvedOptions
import psycopg2

args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME','WORKFLOW_RUN_ID','db_secret_arn'])
# Get the connector name from workflow parameters
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']
workflow_params = boto3.client("glue").get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]
bucket_name = workflow_params['bucket_name']
parent_table = workflow_params['parent_table']
schema = workflow_params['schema']
print ("parent_table :", parent_table, "schema :", schema, "bucket_name :", bucket_name)
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
# Pick up the cold partitions
sql = "select relname, n.nspname from pg_class join pg_namespace n on n.oid = relnamespace where relkind = 'r' and relispartition ='f' and relname like '"+parent_table+"_p%' and n.nspname = '"+schema+"';"
cursor.execute(sql)
result = cursor.fetchall()
# For each cold table
print ("Cold tables :", result)
database = database_secrets['dbname']
for row in result:
    relname = row[0]
    nspname = row[1]
    # Archive the table to S3 
    table_name = nspname+"."+relname
    bucket_path = database+"/"+nspname+"/"+parent_table+"/"+relname+"/data"
    print ("Archiving table "+table_name+" to bucket "+bucket_name+"/"+bucket_path)    
    sql = "SELECT * FROM aws_s3.query_export_to_s3('SELECT * FROM "+table_name+"',aws_commons.create_s3_uri('"+bucket_name+"','"+bucket_path+"'));"
    print("Archive sql: ", sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    print ("Archive result :", result)
    # Drop the cold partition table
    sql = "DROP TABLE "+table_name+";"
    cursor.execute(sql)
    print ("Dropped table :", table_name)

connection.commit()
print ("Archive complete")
cursor.close()
connection.close()




