import os
import boto3
import psycopg2
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger(log_uncaught_exceptions=True)

db_name = os.environ["dbName"]
db_user = os.environ["dbUser"]
db_password = os.environ["dbPassword"]
db_endpoint = os.environ["AuroraEndpoint"]



@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, LambdaContext):
    logger.info({"operation": "Aurora Processing Lambda", "event": event})
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=os.environ["AWS_REGION"]
    )

    try:
        db_password = client.get_secret_value(
            SecretId=os.environ["AURORA_SECRET"]
        )
        conn = psycopg2.connect(host=db_endpoint, dbname=db_name, user=db_user, password=db_password)
        logger.info("SUCCESS: Connection to RDS Aurora instance succeeded")
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            s3location = 's3://' + bucket + '/' + key
            logger.info(s3location)

            sql = "LOAD DATA FROM S3 '" + s3location + "' INTO TABLE import.readings FIELDS TERMINATED BY ',' " \
                "LINES TERMINATED BY '\\n' (uid, timestamp, model, nickname, humidity, luminance, pressure, temperature, moisture_1, moisture_2, moisture_3, voltage);"

            logger.info(sql)

            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                logger.info('Data loaded from S3 into Aurora')
    except:
        logger.error("ERROR: Unexpected error: Could not connect to Aurora instance.")
