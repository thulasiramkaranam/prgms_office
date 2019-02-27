

import json
import boto3
def lambda_handler(event, context):
    # TODO implement
    table = boto3.client('dynamodb', region_name='us-east-1')
    response = table.scan(
        TableName='neo_app_sense_location_match_evnts',
        AttributesToGet=[
        'class1',
        ])
    classes = []
    for i in response['Items']:
        classes.append(i['class1']['S'])
    classes = set(classes)
    output = {}
    output.update({'categories': list(classes)})
      
    return output
    
   
