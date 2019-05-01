

import json
import boto3
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    records = event['data']
    for record in records:
        
        table = boto3.resource('dynamodb', region_name=os.environ['table_region']).Table(os.environ['table_name']) 
        update_exp = ""
        exp_attribute_values = {}
        for key,value in record.items():
            dictt = {}
            dictt.update({"project_code": str(record["project_code"]),
                    "project_name": str(record["project_name"]),
                    "requestor": str(record["requestor"]),
                    "frequency": str(record["frequency"]),
                    "Scrapable": True
            })
        
        for key, value in dictt.items():
            table_key = {}
            table_key.update({'Website': str(record['website_name'])})
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
            "message": "successfully added websites"}

