import json
import boto3
import sys
import os
import logging
import datetime
from datetime import datetime as dtt
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    logger.info(event)
    logger.info("After event")
    client = boto3.client('emr')
    end_time = str(dtt.now())
    table = boto3.resource('dynamodb').Table('NeoApp_Emr_status_monitor')
    try:
        response = {}
        for event in event['Records']:
            if 'OldImage' in event['dynamodb']:
                new_emr_status = event['dynamodb']['NewImage']['Emr_status']['S']
                old_emr_status = event['dynamodb']['OldImage']['Emr_status']['S']
                if old_emr_status == "0" and new_emr_status == "1":
                    emr_id = event['dynamodb']['NewImage']['Emr_id']['S']
                    response = client.terminate_job_flows(
                    JobFlowIds=[
                        emr_id
                            ]
                        )
        if len(response)>0:
            if response['ResponseMetadata']['HTTPStatusCode'] ==200:
                exp_attribute_values = {}
                update_exp = ""
                table_key={}
                table_key.update({'Emr_id': emr_id})
                status = {"Emr_shutdown_time": end_time[:19]}
                for key, value in status.items():
                    update_exp +=  key + ' = :' + key + ','
                    key_placeholder  = ':' + str(key)
                    exp_attribute_values[key_placeholder] = status[key]
               
                update_exp = "SET " + update_exp.rstrip(",")
                table.update_item(Key=table_key,
                                    UpdateExpression=update_exp,                   
                                    ExpressionAttributeValues=exp_attribute_values,
                                    ReturnValues="UPDATED_NEW")
                
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),type(e).__name__, e)
