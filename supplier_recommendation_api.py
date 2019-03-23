

import json
import boto3
import sys
import logging
import datetime
from datetime import datetime as dtt
from boto3.dynamodb.conditions import Key, Attr
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#client = boto3.client('dynamodb')

def lambda_handler(event, context):
   
    try:
   
        from_time = int(event['from_time'])
        to_time = int(event['to_time'])

        table = boto3.client('dynamodb', region_name='us-east-1')
        response = table.scan(
        TableName='neoapp_sense_supplier_recommendation',
        
        FilterExpression='event_time BETWEEN :a and :b',
        ExpressionAttributeValues = {
                ":a": {'N': str(from_time)},
                ":b": {'N': str(to_time)}
            }        
                )
        
            
    except Exception as e:
        print("In the exception of table query")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    output = {}
    if response['Count'] > 0:
        logger.info(response)
        logger.info("After response")
        for row in response['Items']:
            
            logger.info(response['Items'])
            logger.info("After response")
            for record in row['alternate_suppliers']['L']:
                
                for k,v in record['M'].items():
                    listt = []
                    for item in v['L']:
                        dictt = {}
                        dictt.update({"Item_description": item['M']['Item_description']['S'],
                                    "item_no": item['M']['item_no']['S'],
                                    "supplier_name": item['M']['supplier_name']['S'],
                                    "supplier_id": item['M']['supplier_id']['S'],
                                    "supplier_contact": "NA",
                                    "email_link": "NA",
                                    "details": "NA",
                                    "outlook_email": "NA",
                                    "more_details": "NA"
                                })
                        listt.append(dictt)
                output.update({k: listt})
        return output
    else:
        return {}
