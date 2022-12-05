from aws_cdk import (
    # Duration,
    Stack,
    aws_iot as iot,
    aws_dynamodb as dynamodb,
    aws_iam as iam
    # aws_sqs as sqs,
)
from constructs import Construct

class DigitalRepTransformLoad(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Dynamo Table
        readings_table = dynamodb.Table(
            self, "enviro-twin-2",
            partition_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            )
        )
