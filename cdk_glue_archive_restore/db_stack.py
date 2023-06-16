# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
    Aspects,
)
import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_rds as rds
import aws_cdk.aws_iam as iam
from constructs import Construct
import cdk_nag
from cdk_nag import NagSuppressions
from cdk_glue_archive_restore.vpc_stack import VpcStack

class DbStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc_stack: VpcStack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #
        # Create a PostgreSQL database
        #
        self.db_security_group = ec2.SecurityGroup(self, "DBSecurityGroup",
            vpc = vpc_stack.glueVPC,
            allow_all_outbound = True,
            description = "Database security group - allow access to S3",
        )
        self.db_security_group.add_ingress_rule(
            peer = ec2.Peer.ipv4(vpc_stack.cidr),
            connection = ec2.Port.all_tcp(),
        )
        Aspects.of(self).add(cdk_nag.AwsSolutionsChecks())
        self.db_name = 'postgres'
        #
        # Create the bucket policy to allow RDS to export and import from S3
        #
        s3_export_role = iam.Role(self, "pg-s3-export-role",
            assumed_by = iam.ServicePrincipal("rds.amazonaws.com"),
            description = "Allow RDS to export to S3",
        )
        s3_import_role = iam.Role(self, "pg-s3-import-role",
            assumed_by = iam.ServicePrincipal("rds.amazonaws.com"),
            description = "Allow RDS to export to S3",
        )

        s3_exp_policy = iam.Policy(self, "s3-exp-policy",
            force = True,
            statements = [
                iam.PolicyStatement(
                    effect = iam.Effect.ALLOW,
                        actions = [
                                 "s3:GetObject",
                                 "s3:AbortMultipartUpload",
                                 "s3:DeleteObject",
                                 "s3:ListMultipartUploadParts",
                                 "s3:PutObject",
                                 "s3:ListBucket"
                        ],
                    resources = [
                                 "arn:aws:s3:::"+vpc_stack.bucket_name+"/"+self.db_name+"*",
                                 "arn:aws:s3:::"+vpc_stack.bucket_name,
                    ],
                ),
            ],
            roles = [s3_export_role, s3_import_role],
        )

        NagSuppressions.add_resource_suppressions(s3_exp_policy, [
                {"id": 'AwsSolutions-IAM5', "reason": 'Postgres writes to multiple folders in S3 requiring /postgres* suffix ', },
            ]
        )
#
# Create the database and attach the bucket policy
#
        engine = rds.DatabaseInstanceEngine.postgres(version = rds.PostgresEngineVersion.VER_13_10)
        self.glueDB = rds.DatabaseInstance(self, "GlueDB",
            engine = engine,
            database_name = self.db_name,
            instance_type = ec2.InstanceType("t3.medium"),
            vpc = vpc_stack.glueVPC,
            credentials = rds.Credentials.from_generated_secret("postgres"),
            security_groups = [self.db_security_group],
            s3_import_role = s3_import_role,
            s3_export_role = s3_export_role,
            storage_encrypted = True,
        )
        self.glueDB.add_rotation_single_user()
        NagSuppressions.add_resource_suppressions(self.glueDB, [
                {"id": 'AwsSolutions-RDS3', "reason": 'DB is single instance to reduce costs', },
                {"id": 'AwsSolutions-RDS10', "reason": 'Deletion protection deliberately disabled - so that DB deletes when demo is over', },
                {"id": 'AwsSolutions-RDS11', "reason": 'DB uses the default port for this demo', },
            ]
        )


