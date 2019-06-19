import json
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    email = event['email_id']
    dynamodb = boto3.resource('dynamodb')
    dynamodb_table = dynamodb.Table(os.environ['table_name'])
    response = dynamodb_table.query(
                IndexName='email_id-index',
                KeyConditionExpression=Key('email_id').eq(email)
                
                
            )
    if response['Count'] == 0:
        return {"data": []}
    else:
        configurations = []
        for i in response['Items']:
            
            configurations.append(i)
        return {"data": configurations }
    
