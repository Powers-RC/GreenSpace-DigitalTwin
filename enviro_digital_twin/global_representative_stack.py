from aws_cdk import (
    # Duration,
    Stack,
    aws_iot as iot,
    aws_dynamodb as dynamodb,
    aws_iam as iam
    # aws_sqs as sqs,
)
from constructs import Construct
from enviro_digital_twin.transform_representative_data import DigitalRepDataTranform
from enviro_digital_twin.load_transform_data import DigitalRepTransformLoad

class GlobalRepresentative(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ## Transforms digital representatives data 
        DigitalRepDataTranform(scope, "DigitalRepDataTransform")

        ## Loads transformed digital representitive data into strcture db
        DigitalRepTransformLoad(scope, "DigialRepTransformLoad")
