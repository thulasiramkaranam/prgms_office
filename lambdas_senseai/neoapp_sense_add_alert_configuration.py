




import json
import boto3
import copy
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    records = event['data']
    for record in records:
        emails = record['email_id']
        for email in emails:
            
            table = boto3.resource('dynamodb', region_name=os.environ['table_region']).Table(os.environ['table_name']) 
            update_exp = ""
            exp_attribute_values = {}
            dictt = copy.deepcopy(record)
            dictt.pop("email_id")
            table_key = {}
            table_key.update({'email_id': str(email).strip()})
            
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
            "message": "successfully configured alerts"}

