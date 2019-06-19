

import uuid
import json
import boto3
import copy
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    record = event['data']
    
    
    emails = record['email_id']
    for email in emails:
        configuration_id = record['configuration_id']
        table = boto3.resource('dynamodb', region_name=os.environ['table_region']).Table(os.environ['table_name']) 
        update_exp = ""
        exp_attribute_values = {}
        dictt = copy.deepcopy(record)
        dictt.pop("email_id")
        dictt.pop("configuration_id")
        
        table_key = {}
        table_key.update({
                        'configuration_id': configuration_id,
                        'email_id': email
                        })
        
        dictt["comparision_values"] = dictt.pop("values")
        for key, value in dictt.items():
            
            update_exp += key + ' = :' + key + ','
            key_placeholder = ':' + str(key)
            exp_attribute_values[key_placeholder] = dictt[key]
        update_exp = "SET " + update_exp.rstrip(",")
        response = table.update_item(Key=table_key,
                                    UpdateExpression=update_exp,
                                    ExpressionAttributeValues=exp_attribute_values,
                                    ReturnValues="UPDATED_NEW")
    
        response = response['ResponseMetadata']['HTTPStatusCode']
        
            
    #if(response==200):
    return {"status": 200,
            "message": "successfully updated alert configuration"}

