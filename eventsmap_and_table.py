

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
    # TODO implement
    booln = True
    try:
        dynamodb = boto3.resource('dynamodb')
        dynamodb_table = dynamodb.Table('neo_app_sense_location_match_events')
    
     
        # converting to epoch time    
        from_time = int((dtt.strptime(event['from_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        to_time = int((dtt.strptime(event['to_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        if event['event_type'].lower() == 'all':
            
            table = boto3.client('dynamodb', region_name='us-east-1')
            response = table.scan(
            TableName='neo_app_sense_location_match_events',
            
            FilterExpression='epoch_time BETWEEN :a and :b',
            ExpressionAttributeValues = {
                    ":a": {'N': str(from_time)},
                    ":b": {'N': str(to_time)}
                }        
                    )
            booln = False
        
        else:
            response = dynamodb_table.query(
                IndexName='class1-index',
                KeyConditionExpression=Key('class1').eq(event['event_type']),
                
                FilterExpression='epoch_time BETWEEN :a and :b',
                ExpressionAttributeValues = {
                    ":a": from_time,
                    ":b": to_time
                }
            )
            
    except Exception as e:
        print("In the exception of insert lat long")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    output = {'markers': []}
    if response['Count'] == 0:
        dictt = {'status': 404,
                  
                 'message': "No data available"}
        raise Exception(json.dumps(dictt))
    
    if booln is False:
        
        for i in response['Items']:
            for j in i['impacted_locations']['L']:
                dictt = {}
                dictt.update({'lat': float(j['M']['lat_long']['M']['lat']['S']), 'lng': float(j['M']['lat_long']['M']['long']['S']),
                  'Location': j['M']['city']['S'], 'type': i['class1']['S'], 'Summary': i['summary']['S']})
                
                output['markers'].append(dictt) 
        return output
    else:
        print(response)
        for i in response['Items']:
            
            for j in i['impacted_locations']:
                dictt = {}
                dictt.update({'lat':float(j['lat_long']['lat']), 'lng': float(j['lat_long']['long']),'Location': j['city'],
                      'type': i['class1'] ,'Summary': i['summary']})
                
                output['markers'].append(dictt)   
        return output
                     
   
