import json
from marshmallow import Schema, fields, ValidationError
from base64 import b64decode, b64encode
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

#class Reading(Schema):
#    humidity = fields.Number()
#    luminance = fields.Number()
#    moisture_1 = fields.Number()
#    moisture_2 = fields.Number()
#    moisture_3 = fields.Number()
#    pressure = fields.Number()
#    temperature = fields.Number()
#    voltage = fields.Number()
#
#class Map(Schema):
#    map = fields.Nested(Reading())

class Readings(Schema):
    timestamp = fields.Dict(keys=fields.String(), values=fields.DateTime())
    model = fields.Dict(keys=fields.String(), values=fields.String())
    readings = fields.Dict()
    nickname = fields.Dict(keys=fields.String(), values=fields.String())
    uid = fields.Dict(keys=fields.String(), values=fields.String())


logger = Logger(log_uncaught_exceptions=True)

@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, LambdaContext) -> dict:
    logger.info({"operation": "Data Processing Lambda", "event": event})
    successes = 0 
    failures = 0

    records = event["records"]
    output = []
    for record in records:
        data = b64decode(record["data"])
        logger.info({"operation": "Data Processing Lambda", "data": data, "type": type(data)})
        formatted_data = data.decode('utf-8').replace("'", '"')
        logger.info({"operation": "Data Processing Lambda", "formatted data": formatted_data, "type": type(formatted_data)})
        try:
            result = Readings().loads(formatted_data)

            result_csv = '{},{},{},{},{},{},{},{},{},{},{},{},'.format(
                result["uid"]["S"], 
                result["timestamp"]["S"],
                result["model"]["S"],
                result["nickname"]["S"],
                result["readings"]["M"]["humidity"]["N"],
                result["readings"]["M"]["luminance"]["N"],
                result["readings"]["M"]["pressure"]["N"],
                result["readings"]["M"]["temperature"]["N"],
                result["readings"]["M"]["moisture_1"]["N"],
                result["readings"]["M"]["moisture_2"]["N"],
                result["readings"]["M"]["moisture_3"]["N"],
                result["readings"]["M"]["voltage"]["N"]
            )

            logger.info({"operation": "Data Processing Lambda", "CSV Results": result_csv})
            successes += 1
            output.append({
                "recordId": record["recordId"],
                "result": 'Ok',
                "data": b64encode(result_csv.encode('utf-8'))
            })
        except ValidationError as error:
            failures += 1
            logger.info({"Validation Error": error})
            output.append({
                "recordId": record["recordId"],
                "result": 'ProcessingFailed',
                "data": b64encode(formatted_data.encode('utf-8'))
            })

    logger.info(f"Processing Finished:  Successful Records: {successes}, Failed Records: {failures}")
    logger.info(f"Output: {output}")
    return {"records": output}

