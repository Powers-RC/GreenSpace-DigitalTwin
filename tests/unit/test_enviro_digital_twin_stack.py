import aws_cdk as core
import aws_cdk.assertions as assertions

from enviro_digital_twin.digital_representative import EnviroDigitalTwinStack

# example tests. To run these tests, uncomment this file along with the example
# resource in enviro_digital_twin/enviro_digital_twin_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = EnviroDigitalTwinStack(app, "enviro-digital-twin")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
