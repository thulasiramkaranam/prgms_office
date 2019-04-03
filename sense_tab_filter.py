import json
import boto3
def lambda_handler(event, context):
    # TODO implement
    table = boto3.client('dynamodb', region_name='us-east-1')
    response = table.scan(
        TableName='neo_app_sense_evnt_master',
        AttributesToGet=[
        'class1',
        ])
    classes = []
    for i in response['Items']:
        if i['class1']['S'].lower().strip() != 'default':
            classes.append(i['class1']['S'])
    classes = set(classes)
    output = {}
    output.update({'categories': list(classes)})
      
    return output
    
   
