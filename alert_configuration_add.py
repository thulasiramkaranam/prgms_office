import json
import boto3
import copy
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    emails = event['data']['email']
    for email in emails:
        
        table = boto3.resource('dynamodb', region_name='us-east-1').Table('neoapp_sense_alert_configuration') 
        update_exp = ""
        exp_attribute_values = {}
        dictt = copy.deepcopy(event['data'])
        dictt.pop("email")
        table_key = {}
        table_key.update({'email_id': str(email).strip()})
        dictt["alert_definition"] = dictt.pop("definition")
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

