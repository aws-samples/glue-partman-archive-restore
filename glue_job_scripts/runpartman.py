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
connection.autocommit = True
cursor = connection.cursor()
print ("Got connection and cursor")

#
# Run the Partman maintenance SP
#
sql = "CALL partman.run_maintenance_proc();"
cursor.execute(sql)

connection.commit()
print ("partman run_maintenance complete")

cursor.close()
connection.close()

job.commit()