import os
from aws_cdk import (
    Stack,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_iam as iam
)
from constructs import Construct
from dotenv import load_dotenv

load_dotenv()
class GlobalRepresentative(Stack):

    def __init__(self, scope: Construct, construct_id: str, buckets: list, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        powertools_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            id="lambda-powertools",
            layer_version_arn=f"arn:aws:lambda:us-east-1:017000801446:layer:AWSLambdaPowertoolsPythonV2-Arm64:16"
        )

        psycopg2_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            id="lambda-psycopg2",
            layer_version_arn=f"arn:aws:lambda:us-east-1:898466741470:layer:psycopg2-py38:2"
        )

        # Create a vpc
        vpc = ec2.Vpc(self, "GlobalRepresentativeVPC")

        db_user_secret = rds.DatabaseSecret(self, "GlobalRepresentativeDBUserSecret",
            username=os.environ["AURORA_USERNAME"],
            secret_name=os.environ["AURORA_SECRET"],
            exclude_characters='/@" '
        )

        ## Create global datastore using aurora
        cluster = rds.DatabaseCluster(self, "GlobalRepresentativeDB",
            engine=rds.DatabaseClusterEngine.aurora_postgres(version=rds.AuroraPostgresEngineVersion.VER_14_5),
            credentials=rds.Credentials.from_secret(db_user_secret),
            instance_props=rds.InstanceProps(
                # optional , defaults to t3.medium
                instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM),
                vpc_subnets=ec2.SubnetSelection(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
                ),
                vpc=vpc
            ),
            default_database_name=os.environ["AURORA_DB_NAME"]
        )
        
        # Aurora data processor
        aurora_processing_function = lambda_.Function(self, f"AuroraDataProcessor",
            code=lambda_.Code.from_asset(
                "./enviro_digital_twin/lambdas/aurora_processing_lambda/.build/aurora-lambda-deployment.zip"
            ),
            runtime=lambda_.Runtime.PYTHON_3_9,  # required
            handler="aurora_processing_lambda.lambda_handler",
            environment={
                "AuroraEndpoint": cluster.cluster_endpoint.socket_address,
                "dbName": os.environ["AURORA_DB_NAME"],
                "dbUser": os.environ["AURORA_USERNAME"],
                "dbSecretName": os.environ["AURORA_SECRET"]
            },
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.ONE_MONTH,
            layers=[powertools_layer, psycopg2_layer],
        )

        self.processing_lambda = aurora_processing_function

        aurora_processing_function.add_to_role_policy(iam.PolicyStatement(
            resources=buckets,
            actions=["lambda:InvokeFunction"]
        ))
        for idx, arn in enumerate(buckets):
            bucket = s3.Bucket.from_bucket_arn(self, f"enviro-twin-{idx + 1}-TransformBucket", arn)
            bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3n.LambdaDestination(aurora_processing_function)
            )

        db_user_secret.grant_read(aurora_processing_function)
