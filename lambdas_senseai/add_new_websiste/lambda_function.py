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


def add_website_to_db(event):

    records = event['data']
    for record in records:
        
        table = boto3.resource('dynamodb', region_name=os.environ['region_name']).Table(os.environ['table_name']) 
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
        

            
def base64_to_json(input_string):
    
    decoded_string = base64.b64decode(input_string)
    io_string = io.BytesIO()
    io_string.write(decoded_string)
    io_string.seek(0)
    dataframe = pd.read_csv(io_string)
    
    input = {"data": []}
    for i in range(len(dataframe)):
        row = dataframe.iloc[i]
        dictt = {}
        
        dictt.update({
                    "website_name": row["website_url"],
                    "project_code": row["project_code"],
                    "project_name": row["project_name"],
                    "requestor": row["requestor"],
                    "frequency": row["refresh_frequency"]

                    })
        input['data'].append(dictt)
    add_website_to_db(input)


def lambda_handler(event, context):
    
    if 'source_file' in event['data']:
        base64_to_json(event['data']['source_file'])
    else:
        add_website_to_db(event)      
    
    return {"status": 200,
            "message": "successfully configured alerts"}

