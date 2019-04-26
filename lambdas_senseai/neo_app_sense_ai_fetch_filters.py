

import json
import boto3
import os
def lambda_handler(event, context):
    # TODO implement
    table = boto3.client('dynamodb', region_name=os.environ['table_region'])
    response = table.scan(
        TableName=os.environ['table_name'],
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
    
   
