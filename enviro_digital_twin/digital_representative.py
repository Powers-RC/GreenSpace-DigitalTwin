from aws_cdk import (
    # Duration,
    Stack,
    Size,
    Duration,
    RemovalPolicy,
    BundlingOptions,
    aws_iot as iot,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_s3 as s3,
    # aws_kinesisfirehose as firehose,
    aws_kinesisfirehose_alpha as firehose,
    aws_logs as logs,
    aws_kms as kms,
    aws_lambda as lambda_,
    aws_kinesis as kinesis,
    aws_kinesisfirehose_destinations_alpha as destinations,
    # aws_kinesis as kinesis,
    aws_sqs as sqs,
)
from constructs import Construct
from aws_cdk.aws_lambda_event_sources import DynamoEventSource, SqsDlq
from os import path, getcwd




class DigitalRepresentative(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        rep_id = str(kwargs.pop("rep_id"))
        super().__init__(scope, construct_id, **kwargs)

        powertools_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            id="lambda-powertools",
            layer_version_arn=f"arn:aws:lambda:us-east-1:017000801446:layer:AWSLambdaPowertoolsPythonV2-Arm64:16"
        )

        ######  Store Section ######
        # Create IOT Thing
        thing_name ='enviro-twin-' + rep_id
        cfn_thing = iot.CfnThing(self, "EnviroTwin-" + rep_id,
            thing_name=thing_name,
        )

        # Create Certificate
        # Currently created in console. Could create and store and secrets manager then load... get workng first
        certificate_arn="arn:aws:iot:us-east-1:040001342592:cert/72cc9d83457eaa6c4c9a51c8ccf5a04f0f8d6eb9caa43f0d71730236140876b1"

        # Create Policy
        document = {
            "Version": "2012-10-17",
            "Statement": [
                {  
                    "Effect":"Allow",
                    "Action":["iot:*"],
                    "Resource":[ "*" ],
                    "Condition": {
                        "Bool": {
                            "iot:Connection.Thing.IsAttached": ["true"]
                        }
                    }
                }
            ]
        }
        policy = iot.CfnPolicy(self, "IOTCertPolicyTwin",  policy_document=document, policy_name="IOTCertPolicyTwin-" + rep_id)

        # Attch policy to certificate
        iot.CfnPolicyPrincipalAttachment(
            self,
            id="IOTCertPolicyTwin2Attach",
            policy_name=policy.policy_name, 
            principal=certificate_arn
        ).add_depends_on(policy)

        # Attch certificate to things
        iot.CfnThingPrincipalAttachment(self, id="X509CertTwin2Attach",
            principal=certificate_arn,
            thing_name=cfn_thing.thing_name
        ).add_depends_on(cfn_thing)


        #Create role to allow iot access dynamodb service
        role = iam.Role(self, "Twin2DigitalEntity",
            assumed_by=iam.ServicePrincipal("iot.amazonaws.com")
        )

        role.add_to_policy(iam.PolicyStatement(
            resources=["*"],
            actions=["dynamodb:*"]
        ))

        # Create Dynamo Table
        stream = kinesis.Stream(self, "DDBDataStream-" + thing_name)
        readings_table = dynamodb.Table(
            self, thing_name,
            table_name=thing_name,
            partition_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            kinesis_stream=stream
        )

        # Create rule to write data pubished data to dynamo db
        sql_stmt = f"SELECT * FROM 'enviro/{thing_name}'"
        iot.CfnTopicRule(self, id="TwinPublishToDynamoDB",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                actions=[iot.CfnTopicRule.ActionProperty(
                    dynamo_d_bv2=iot.CfnTopicRule.DynamoDBv2ActionProperty(
                        put_item=iot.CfnTopicRule.PutItemInputProperty(
                            table_name=readings_table.table_name
                        ),
                        role_arn=role.role_arn
                    )
                )],
                sql=sql_stmt,
                rule_disabled=False
            )
        )

        ###### Transform Section ######
        # Lambda stream processor
        stream_processing_function = lambda_.Function(self, f"{thing_name}-StreamProcessor",
            code=lambda_.Code.from_asset("./enviro_digital_twin/lambdas/stream_processing_lambda/"),  # required
            runtime=lambda_.Runtime.PYTHON_3_9,  # required
            handler="stream_processing_lambda.lambda_handler",
            environment={
                "THING_NAME": thing_name
            },
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.ONE_MONTH,
            layers=[powertools_layer],
            timeout=Duration.minutes(1)
        )

        dead_letter_queue = sqs.Queue(self, f"{thing_name}-StreamDeadLetterQueue")
        stream_processing_function.add_event_source(DynamoEventSource(readings_table,
            starting_position=lambda_.StartingPosition.TRIM_HORIZON,
            batch_size=5,
            bisect_batch_on_error=True,
            on_failure=SqsDlq(dead_letter_queue),
            retry_attempts=10,
        ))

        # Bucket Setup
        bucket = s3.Bucket(self,
            f"{thing_name}-TransformBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        self.bucket = bucket

        backup_bucket = s3.Bucket(self,
            f"{thing_name}-BackupTransformBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
        log_group = logs.LogGroup(self, 
            f"{thing_name}-Transform",
            removal_policy=RemovalPolicy.DESTROY
        )


        # TODO: Make the build contents hidden & update code path
        data_processing_function = lambda_.Function(self, f"{thing_name}-DataProcessor",
            code=lambda_.Code.from_asset("./enviro_digital_twin/lambdas/data_processing_lambda/data-lambda-deployment.zip"),  # required
            runtime=lambda_.Runtime.PYTHON_3_9,  # required
            handler="data_processing_lambda.lambda_handler",
            environment={
                "THING_NAME": thing_name
            },
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.ONE_MONTH,
            layers=[powertools_layer],
            timeout=Duration.minutes(1)
        )

        processor = firehose.LambdaFunctionProcessor(data_processing_function,
            buffer_interval=Duration.seconds(60),
            buffer_size=Size.mebibytes(1),
            retries=1
        )

        key = kms.Key(self, "Key",
            removal_policy=RemovalPolicy.DESTROY
        )

        backup_key = kms.Key(self, "BackupKey",
            removal_policy=RemovalPolicy.DESTROY
        )

        #Kenesis Setup
        delivery_stream = firehose.DeliveryStream(self, f"{thing_name}-DeliveryStream",
            delivery_stream_name=f"{thing_name}-DeliveryStream-1",
            destinations=[destinations.S3Bucket(bucket,
                logging=True,
                log_group=log_group,
                processor=processor,
                compression=destinations.Compression.GZIP,
                data_output_prefix=f"{thing_name}" + "/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}",
                error_output_prefix=f"{thing_name}" + "-FirehoseFailures/!{firehose:error-output-type}/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}",
                buffering_interval=Duration.seconds(60),
                buffering_size=Size.mebibytes(1),
                encryption_key=key,
                s3_backup=destinations.DestinationS3BackupProps(
                    mode=destinations.BackupMode.ALL,
                    bucket=backup_bucket,
                    compression=destinations.Compression.ZIP,
                    data_output_prefix=f"{thing_name}" + "-Backup/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}",
                    error_output_prefix=f"{thing_name}" + "-Backup-FirehoseFailures/!{firehose:error-output-type}/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}",
                    buffering_interval=Duration.seconds(60),
                    buffering_size=Size.mebibytes(1),
                    encryption_key=backup_key
                )
            )]
        )

        stream_processing_function.add_to_role_policy(iam.PolicyStatement(
            resources=[delivery_stream.delivery_stream_arn],
            actions=["firehose:*"]
        ))
