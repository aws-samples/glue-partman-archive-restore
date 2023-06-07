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
table_filter = workflow_params['table_filter']
schema = workflow_params['schema']

print ("connection :", connection, "table_filter :", table_filter, "schema :", schema, "bucket_name :", bucket_name)
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
cursor = connection.cursor()
print ("Got connection and cursor")

#
# Pick up the cold partitions
#
sql = "select relname, n.nspname from pg_class join pg_namespace n on n.oid = relnamespace where relkind = 'r' and relispartition ='f' and relname like '"+table_filter+"' and n.nspname = '"+schema+"';"
cursor.execute(sql)
result = cursor.fetchall();

#
# For each cold table
#
# result = [{'relname': 'ticket_purchase_hist_p2021_01', 'nspname' : 'dms_sample'},]
print ("Cold tables :", result)

for row in result:
    relname = row[0]
    nspname = row[1]
    #
    # Archive the table to S3 
    #
    table_name = nspname+"."+relname
    bucket_path = database+"/"+nspname+"/"+relname+"/data"
    print ("Archiving table "+table_name+" to bucket "+bucket_name+"/"+bucket_path)
    
    sql = "SELECT * FROM aws_s3.query_export_to_s3('SELECT * FROM "+table_name+"',aws_commons.create_s3_uri('"+bucket_name+"','"+bucket_path+"'));"
    print("Archive sql: ", sql)
    cursor.execute(sql)
    result = cursor.fetchall();

    print ("Archive result :", result)
    #
    # Drop the cold partition table
    #
    sql = "DROP TABLE "+table_name+";"
    cursor.execute(sql)
    print ("Dropped table :", table_name)

connection.commit()
print ("Archive complete")

cursor.close()
connection.close()

job.commit()



