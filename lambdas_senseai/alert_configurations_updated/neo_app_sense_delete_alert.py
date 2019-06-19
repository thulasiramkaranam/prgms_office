

import json
import os
import boto3
def lambda_handler(event, context):
    # TODO implement
    email_id = event['email_id']
    configuration_id = event['configuration_id']
    dynamodb = boto3.resource('dynamodb', region_name=os.environ['table_region'])
    table = dynamodb.Table(os.environ['table_name'])
    response = table.delete_item(
        Key={
            'email_id': email_id,
            'configuration_id': configuration_id
        }
        
    )
    return {
        'statusCode': 200,
        'body': "Configuration deleted successfully"
    }
