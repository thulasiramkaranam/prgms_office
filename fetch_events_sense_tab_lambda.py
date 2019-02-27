

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
        dynamodb_table = dynamodb.Table('neo_app_sense_location_match_evnts')
    
     
        # converting to epoch time    
        from_time = int((dtt.strptime(event['from_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        to_time = int((dtt.strptime(event['to_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        if event['event_type'].lower() == 'all':
            
            table = boto3.client('dynamodb', region_name='us-east-1')
            response = table.scan(
            TableName='neo_app_sense_location_match_evnts',
            
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
            logger.info(response)
            logger.info("After response")
            
    except Exception as e:
        print("In the exception of insert lat long")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    output = {'markers': []}
    if response['Count'] == 0:
        dictt = {'status': 404,
                  
                 'message': "No data available"}
        raise Exception(json.dumps(dictt))
    
    if booln is False:
        table_data = []
        for i in response['Items']:
            
            table_data_dictt = {}
            table_data_location = ""
            print(i)
            print("After i")
            for j in i['impacted_locations']['L']:
                
                dictt = {}
                if 'NULL'  not in j['M']['city'] and 'none' not in j['M']['city']['S']:
                    location = j['M']['city']['S']
                elif 'NULL' not in j['M']['state'] and 'none' not in j['M']['state']['S']:
                    location = j['M']['state']['S']
                else:
                    location = j['M']['Country']['S']
                table_data_location += location+", "
                dictt.update({'lat': float(j['M']['lat_long']['M']['lat']['S']), 'lng': float(j['M']['lat_long']['M']['long']['S']),
                  'Location': location, 'type': i['class1']['S'], 'Summary': i['summary']['S']})
                
                output['markers'].append(dictt) 
            table_data_dictt.update({'event_Id': i['event_id']['S'],"event_desc":i["summary"]['S'], "severity": i["severity"]['S'],
                                  "location": table_data_location.rstrip(), 
                                  "impacted_entities":i['entities_impacted']['N'],"keywords": ", ".join(key['S'] for key in i['tags']['L'])
            })
            
            table_data.append(table_data_dictt)
        output.update({'table_data': table_data})
        return output
    else:
        print(response)
        table_data = []
        for i in response['Items']:
            
            table_data_dictt = {}
            table_data_location = ""
            for j in i['impacted_locations']:
                dictt = {}
                print(j)
                print("After j")
                if j['city'] is not None:
                    location = j['city']
                elif j['state'] is not None:
                    location = j['state']
                else:
                    location = j['Country']
                table_data_location += location+", "
                dictt.update({'lat':float(j['lat_long']['lat']), 'lng': float(j['lat_long']['long']),'Location': location,
                      'type': i['class1'] ,'Summary': i['summary']})
                
                output['markers'].append(dictt)   
            table_data_dictt.update({'event_Id': i['event_id'],"event_desc":i["summary"], "severity": i["severity"],
                                  "location": table_data_location.rstrip(), 
                                  "impacted_entities":i['entities_impacted'],"keywords": ", ".join(key for key in i['tags'])
            })
            table_data.append(table_data_dictt)
        output.update({'table_data': table_data})
        return output
                     
   
