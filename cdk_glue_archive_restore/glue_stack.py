# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
)
import aws_cdk as cdk
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_rds as rds
import aws_cdk.aws_iam as iam
import aws_cdk.aws_glue as glue
from constructs import Construct

from cdk_glue_archive_restore.vpc_stack import VpcStack
from cdk_glue_archive_restore.db_stack import DbStack

class GlueStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc_stack: VpcStack, db_stack:DbStack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        db_secret = db_stack.glueDB.secret

        jdbc_url = "jdbc:postgresql://"+db_stack.glueDB.instance_endpoint.socket_address+"/"+db_stack.db_name

        #
        # Upload the scripts from local file system to S3
        #

        s3deploy.BucketDeployment(self, "Deploy Scripts",
                        sources = [
                            s3deploy.Source.asset("glue_job_scripts"),
                        ],
                        destination_bucket = vpc_stack.archive_bucket,
                        destination_key_prefix = "scripts/",
        )


        #
        # Create a security group to allow Glue to reach all resources on the subnet
        #
        glue_security_group = ec2.SecurityGroup(self, "GlueSecurityGroup",
            vpc = vpc_stack.glueVPC,
            allow_all_outbound = True,
            description = "Allow Glue ETL jobs access to subnet resources",
        )
        glue_security_group.add_ingress_rule(
            peer = ec2.Peer.any_ipv4(),
            connection = ec2.Port.all_tcp(),
        )
        #
        # Create a Role that Glue will use to access Postgres, SecretManager and S3
        # These are managed policies, except the CRCEZ
        #
        glue_role = iam.Role(self, "GlueRoleWithSecrets",
            assumed_by = iam.ServicePrincipal("glue.amazonaws.com"),
            description = "Allow Glue to access S3 SecretsManager and RDS",
        )
        glue_role.attach_inline_policy(
            iam.Policy(self, "Glue S3 EZ CRC Policy",
                statements = [
                    iam.PolicyStatement(
                        effect = iam.Effect.ALLOW,
                        actions = ["s3:GetObject", "s3:PutObject"],
                        resources = ["arn:aws:s3:::aws-glue-assets-"+self.account+"-"+self.region+"/*"],
                    ),
                    iam.PolicyStatement(
                        effect = iam.Effect.ALLOW,
                        actions = ["secretsmanager:GetSecretValue",],
                        resources = ["*"],
                    ),
                    iam.PolicyStatement(
                        effect = iam.Effect.ALLOW,
                        actions = ["s3:GetObject", "s3:PutObject"],
                        resources = ["arn:aws:s3:::"+vpc_stack.bucket_name+"/*"],
                    ),
                ],
            ),
        )
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))
        #
        # Create an AWS Glue connection to the postgres database
        #
        connection_name = "Database A"
        glue_connection = glue.CfnConnection(self, connection_name,
            catalog_id = self.account,
            connection_input = glue.CfnConnection.ConnectionInputProperty(
                name = connection_name,
                description = "Connector to postgres database",
                connection_type = "JDBC",
                connection_properties = {
                    "JDBC_CONNECTION_URL" : jdbc_url,
                    "SECRET_ID" : db_secret.secret_name,
                    "JDBC_ENFORCE_SSL" : False,
                    "KAFKA_SSL_ENABLED" : False,
                },
                physical_connection_requirements = glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    security_group_id_list = [db_stack.db_security_group.security_group_id, glue_security_group.security_group_id],
                    subnet_id = vpc_stack.private_subnets.subnets[1].subnet_id,
                    availability_zone = vpc_stack.private_subnets.subnets[1].availability_zone,
                )
            )
        )
        #
        # Create the glue jobs
        #
        archive_job = glue.CfnJob(self, "Archive Cold Tables",
            command = glue.CfnJob.JobCommandProperty(
                name = "glueetl",
                python_version = "3",
                script_location = "s3://"+vpc_stack.bucket_name+"/scripts/runarchive.py",
            ),
            role = glue_role.role_name,
            connections = glue.CfnJob.ConnectionsListProperty(
                connections = [connection_name],
            ),
            description = "Archive cold partition table to S3",
            execution_property = glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version = "4.0",
            max_retries = 0,
            name = "Archive Cold Tables",
            notification_property = glue.CfnJob.NotificationPropertyProperty(
                notify_delay_after=123
            ),
            worker_type = "G.1X",
            number_of_workers = 10,
            timeout = 10,
            default_arguments={"--additional-python-modules" : "psycopg2-binary", "--job-bookmark-option": "job-bookmark-disable"},
        )
        restore_job = glue.CfnJob(self, "Restore From S3",
            command = glue.CfnJob.JobCommandProperty(
                name = "glueetl",
                python_version = "3",
                script_location = "s3://"+vpc_stack.bucket_name+"/scripts/runrestore.py",
            ),
            role = glue_role.role_name,
            connections = glue.CfnJob.ConnectionsListProperty(
                connections = [connection_name],
            ),
            description = "Restore partition table from S3",
            execution_property = glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version = "4.0",
            max_retries = 0,
            name = "Restore From S3",
            notification_property = glue.CfnJob.NotificationPropertyProperty(
                notify_delay_after=123
            ),
            worker_type = "G.1X",
            number_of_workers = 10,
            timeout = 10,
            default_arguments={"--additional-python-modules" : "psycopg2-binary", "--job-bookmark-option": "job-bookmark-disable"}
        )
        partman_job = glue.CfnJob(self, "Partman run-maintenance",
            command = glue.CfnJob.JobCommandProperty(
                name = "glueetl",
                   python_version = "3",
                   script_location = "s3://"+vpc_stack.bucket_name+"/scripts/runpartman.py",
            ),
            role = glue_role.role_name,
            connections = glue.CfnJob.ConnectionsListProperty(
                connections = [connection_name],
            ),
            description = "Partman run.maintenance",
            execution_property = glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version = "4.0",
            max_retries = 0,
            name = "Partman run maintenance",
            notification_property = glue.CfnJob.NotificationPropertyProperty(
                notify_delay_after=123
            ),
            worker_type = "G.1X",
            number_of_workers = 10,
            timeout = 10,
            default_arguments={"--additional-python-modules" : "psycopg2-binary", "--job-bookmark-option": "job-bookmark-disable"},
        )
        #
        # Create the Archive Glue Workflow, its triggers, and link them together
        #
        archive_workflow = glue.CfnWorkflow(self, "Maintain and Archive",
            default_run_properties = {
                "connection" : connection_name,
                "schema" : "dms_sample",
                "bucket_name" : vpc_stack.bucket_name,
                "table_filter" : "ticket_purchase_hist_p%",
                "region" : self.region,
            },
            description = "Maintain and archive cold partitionz",
            max_concurrent_runs = 1,
            name = "Maintain and Archive",
        )
        #
        # Create a Glue Trigger to start the archive workflow
        #
        maintain_trigger = glue.CfnTrigger(self, "MaintainTrigger",
            name = "Trigger-" + partman_job.name,
            workflow_name = archive_workflow.name,
            actions = [
                glue.CfnTrigger.ActionProperty(
                    job_name = partman_job.name,
                    timeout = 120,
                ),
            ],
            type = "ON_DEMAND",
        )
        maintain_trigger.add_dependency(archive_workflow)
        #
        # Trigger to drop the table once the archive job has completed
        #
        archive_trigger = glue.CfnTrigger(self, "Archive Trigger",
            name = "Trigger-" + archive_job.name,
            workflow_name = archive_workflow.name,
            actions = [
                glue.CfnTrigger.ActionProperty(
                    job_name = archive_job.name,
                    timeout = 120,
                ),
            ],
            predicate = glue.CfnTrigger.PredicateProperty(
                conditions = [
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator = "EQUALS",
                        job_name = partman_job.name,
                        state = "SUCCEEDED",
                    ),
                ],
                logical = "ANY",
            ),
            type = "CONDITIONAL",
            start_on_creation = True,
        )
        archive_trigger.add_dependency(archive_workflow)
        archive_trigger.add_dependency(partman_job)
        #
        # Create the Restore Glue Workflow, its triggers, and link them together
        #
        restore_workflow = glue.CfnWorkflow(self, "Restore Workflow",
            default_run_properties = {
                "connection" : connection_name,
                "restore_date" : "2020_01_01",
                "bucket_name" : vpc_stack.bucket_name,
                "parent_table" : "dms_sample.ticket_purchase_hist",
                "region" : self.region,
            },
            description = "Restore the partition table from S3 archive",
            max_concurrent_runs = 1,
            name = "Restore From S3",
        )
        #
        # Create a Glue Trigger to start the restore workflow
        #
        create_table_trigger = glue.CfnTrigger(self, "Restore Trigger",
            name = "Trigger-" + restore_job.name,
            workflow_name = restore_workflow.name,
            actions = [
                glue.CfnTrigger.ActionProperty(
                    job_name = restore_job.name,
                    timeout = 120,
                ),
            ],
            type = "ON_DEMAND",
        )
        create_table_trigger.add_dependency(restore_workflow)




