# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import sys
import boto3
import json
import psycopg2
from awsglue.utils import getResolvedOptions

def get_db_connection(db_secret_arn):
    # Get the database connection information from the secret stored in secrets manager
    secrets_manager_client = boto3.client('secretsmanager')
    secret_value = secrets_manager_client.get_secret_value(SecretId=db_secret_arn)
    database_secrets = json.loads(secret_value['SecretString'])
    return psycopg2.connect(database=database_secrets['dbname'], user = database_secrets['username'], 
                            password = database_secrets['password'], host = database_secrets['host'], 
                            port = database_secrets['port'])

args = getResolvedOptions(sys.argv, ['db_secret_arn'])
connection = get_db_connection(args['db_secret_arn'])
connection.autocommit = True
cursor = connection.cursor()
print ("Got connection and cursor")
# Run the Partman maintenance SP
sql = "CALL partman.run_maintenance_proc();"
cursor.execute(sql)
print ("partman run_maintenance complete")
cursor.close()
connection.close()    