import json
import urllib.parse
import boto3

print('Loading function')

step_func = boto3.client('stepfunctions')
state_machine_arn = 'arn:aws:states:us-east-1:481665108850:stateMachine:enrollment-data-state-machine'


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        response = step_func.start_execution(
            stateMachineArn = state_machine_arn
            )
        print("CONTENT TYPE: " + response['ContentType'])
        return response['ContentType']
        
    except Exception as e:
        print(e)
        print('Error running the crawler')
        raise e
