

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

def transform(data,typ):
    if typ == 'lat':
        if 'S' in data:
            splt = data.split('.')
            splt = splt[0]+'.'+splt[1][:3]
            
            lat = -1 * float(splt)
            return lat
        elif 'N' in data:
            
            splt = data.split('.')
            splt = splt[0]+'.'+splt[1][:3]
            print(splt)
            lat = 1 * float(splt)
            return lat
            
        else:
            return float(data)
        
    else:
        if 'W' in data:
            splt = data.split('.')
            splt = splt[0]+'.'+splt[1][:3]
            
            lng = -1 * float(splt)
            return lng
            
        elif 'E' in data:
            splt = data.split('.')
            splt = splt[0]+'.'+splt[1][:3]
            
            lng = 1 * float(splt)
            return lng
        else:
            return float(data)

def lambda_handler(event, context):
    # TODO implement
    booln = True
    try:
        dynamodb = boto3.resource('dynamodb')
        dynamodb_table = dynamodb.Table('neo_app_sense_event_master')
    
     
        # converting to epoch time    
        from_time = int((dtt.strptime(event['from_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        to_time = int((dtt.strptime(event['to_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        if event['event_type'].lower() == 'all':
            
            table = boto3.client('dynamodb', region_name='us-east-1')
            response = table.scan(
            TableName='neo_app_sense_event_master',
            
            FilterExpression='epoch_time BETWEEN :a and :b',
            ExpressionAttributeValues = {
                    ":a": {'N': str(from_time)},
                    ":b": {'N': str(to_time)}
                }        
                    )
            booln = False
            
        
        else:
            response = dynamodb_table.query(
                IndexName='class2-index',
                KeyConditionExpression=Key('class2').eq(event['event_type']),
                
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
        dictt = {
                        "markers": [
                           
                        ],
                        "table_data": [
                        
                        ]
                    } 
 

        return dictt
    
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
                if location != 'none' and i['class2']['S'].lower().strip() != 'default':
                    table_data_location += location+", "
                    dictt.update({'lat': transform(j['M']['lat_long']['M']['lat']['S'], 'lat'), 'lng': transform(j['M']['lat_long']['M']['long']['S'], 'lng'),
                      'Location': location, 'type': i['class2']['S'], "severity": i["severity"]['S'],'Summary': i['headline']['S']})
                
                    output['markers'].append(dictt) 
            if len(table_data_location) > 0:
                
                table_data_dictt.update({'event_Id': i['event_id']['S'],"event_desc":i["summary"]['S'], "severity": i["severity"]['S'],
                                  "location": table_data_location.rstrip(), 
                                  "headline": i['headline']['S'],
                                   "event_date": i['event_date']['S'],
                                   "keywords": ", ".join(key['S'].replace("'",'').lstrip() for key in i['tags']['L'])
                                  
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
                if 'NULL' not in j['city'] and 'none' not in j['city']:
                    location = j['city']
                elif 'NULL' not in j['state'] and 'none' not in j['state']:
                    location = j['state']
                else:
                    location = j['Country']
                print(location)
                print("After location")
                if location != 'none':
                    table_data_location += location+", "
                    dictt.update({'lat':transform(j['lat_long']['lat'], 'lat'), 'lng': transform(j['lat_long']['long'], 'lng'),'Location': location,
                      'type': i['class2'] , "severity": i["severity"], 'Summary': i['headline']})
                
                    output['markers'].append(dictt) 
            if len(table_data_location) > 0:
                table_data_dictt.update({'event_Id': i['event_id'],"event_desc":i["summary"], "severity": i["severity"],
                                   "headline": i['headline'],
                                   "event_date": i['event_date'],
                                  "location": table_data_location.rstrip(), 
                                  
                                  "keywords": ", ".join(key.replace("'",'').lstrip() for key in i['tags'])
                    })
                table_data.append(table_data_dictt)
        output.update({'table_data': table_data})
        return output
                     
   
