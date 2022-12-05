import os
import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger(log_uncaught_exceptions=True)

@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, LambdaContext):
    logger.info({"operation": "Stream Processing Lambda", "event": event})
    firehose = boto3.client('firehose')

    fireHoseInput = []
    for record in event["Records"]:
        if record["eventName"] == "INSERT":
            fireHoseInput.append({ "Data": str(record["dynamodb"]["NewImage"])})

    if fireHoseInput:
        thing_name = os.environ["THING_NAME"]
        params = {
            "DeliveryStreamName": f'{thing_name}-DeliveryStream-1',
            "Records": fireHoseInput
        }

        result = firehose.put_record_batch(**params)
