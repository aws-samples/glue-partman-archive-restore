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
import aws_cdk.aws_glue as glue
from constructs import Construct
import cdk_nag
from cdk_nag import NagSuppressions
class VpcStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Aspects.of(self).add(cdk_nag.AwsSolutionsChecks())
        #
        # Create the S3 bucket used for the archive
        #
        self.bucket_name = "glue-archive-"+self.account+"-"+self.region;
        self.archive_bucket = s3.Bucket(self, "GlueBucket",
            versioned=True,
            bucket_name = self.bucket_name,
            removal_policy = cdk.RemovalPolicy.DESTROY,
            auto_delete_objects = True,
            server_access_logs_prefix = "AccessLog",
            enforce_ssl = True,
        )

        #
        # Create a VPC for the solution
        # with 1 private subnet per AZ
        # S3 Gateway endpoint so that Glue can communicate with S3
        #
        self.cidr = '10.0.0.0/16'
        self.glueVPC = ec2.Vpc(self, 'NewGlueVPC',
            ip_addresses = ec2.IpAddresses.cidr(self.cidr),
            subnet_configuration = [
                ec2.SubnetConfiguration(            # Needed for Cloud9 and to download test data
                    name = 'PublicGlue1a',
                    subnet_type = ec2.SubnetType.PUBLIC,
                    cidr_mask = 24
                ),
                ec2.SubnetConfiguration(            # Database and Glue jobs run here
                    name = 'PrivateGlue1a',
                    subnet_type = ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask = 24
                )
            ],
            nat_gateways = 1,                       # Needed for Cloud9 and to download test data
            gateway_endpoints={
                "s3" : ec2.GatewayVpcEndpointOptions(
                    service = ec2.GatewayVpcEndpointAwsService.S3
                )
            },            
        )      
        # 
        # Add interface endpoint for secrets manager so that the glue job can extract the DB credentials
        #
        self.glueVPC.add_interface_endpoint('SecretsManagerEndpoint',
                                          service = ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER)
        self.private_subnets = self.glueVPC.select_subnets(subnet_type = ec2.SubnetType.PRIVATE_WITH_EGRESS)

        self.glueVPC.add_flow_log('FlowLogCloudWatch', traffic_type = ec2.FlowLogTrafficType.REJECT)

        #
        # Supress the CDK Nag warning - that CDK does not know the secrets manager endpoint's default security group setting because its set by an intrinsic function
        #
        NagSuppressions.add_resource_suppressions_by_path(self,
                '/vpcstack/NewGlueVPC/SecretsManagerEndpoint/SecurityGroup/Resource',[
                {"id": 'CdkNagValidationFailure', "reason": 'Security Manager security group needs no explicit port range set', },
           ]
        )


