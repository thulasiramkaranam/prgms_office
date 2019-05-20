import json
import boto3
import copy
import logging
import base64
import os
import pandas as pd
import io
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def add_configuration_to_db(event):


    records = event['data']
    
    
    if 0 in records[0]['event_attributes']:
        records[0]['event_attributes'].remove(0)
    
    
    # fix ends
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
            logger.info(response)
            
def base64_to_json(input_string):
    
    decoded_string = base64.b64decode(input_string)
    io_string = io.BytesIO()
    io_string.write(decoded_string)
    io_string.seek(0)
    dataframe = pd.read_csv(io_string)
    

    for i in range(len(dataframe)):
        row = dataframe.iloc[i]
        dictt = {}
        input = {"data": []}
        dictt.update({"project_code": row["Project Code"], "start_date": row['start_date'],
                    "requestor": row['requestor'], "description": row["description"],
                    "email_id": [row['email']],
                    "sources": [ii.strip() for ii in row['sources'].split(",")],
                    "event_attributes": [ea.strip() for ea in row['event_attribute'].split(",")],
                    "expression_type": str(row['data_type']),
                    "comparator": str(row['operator']),
                    "values": [v.strip() for v in row['values'].split(",")]

                    })
        input['data'].append(dictt)
        add_configuration_to_db(input)



def lambda_handler(event, context):
    
    if 'source_file' in event['data']:
        base64_to_json(event['data']['source_file'])
    else:
        add_configuration_to_db(event)      
    
    return {"status": 200,
            "message": "successfully configured alerts"}

