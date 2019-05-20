

import json
import boto3
import sys
import logging
import datetime
import os
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
            
def filter_severity_table_data(event,table_data_dictt,table_data):
    if event['severity']!='all':
            
        if event['severity']=='<=4':
            if int(table_data_dictt['severity']) <= 4:
                table_data.append(table_data_dictt)
                
        elif event['severity']=='4<=7':
            if 4< int(table_data_dictt['severity']) <=7:
                table_data.append(table_data_dictt)
        else:
            if  int(table_data_dictt['severity']) > 7:
                table_data.append(table_data_dictt)
                
    else:
        table_data.append(table_data_dictt)
        
def filter_severity_marker_data(event,dictt,output):
    if event['severity']!='all':
        if event['severity']=='<=4':
            if int(dictt['severity']) <= 4:
                output['markers'].append(dictt)
                
        elif event['severity']=='4<=7':
            if 4< int(dictt['severity']) <=7:
                output['markers'].append(dictt)
        else:
            if  int(dictt['severity']) > 7:
                output['markers'].append(dictt)
    else:
        output['markers'].append(dictt)

def lambda_handler(event, context):
    # TODO implement
    booln = True
    try:
        dynamodb = boto3.resource('dynamodb')
        dynamodb_table = dynamodb.Table(os.environ['event_table_name'])
        dynamodb_table_location = dynamodb.Table(os.environ['location_table_name'])
    
     
        # converting to epoch time    
        from_time = int((dtt.strptime(event['from_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        to_time = int((dtt.strptime(event['to_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        scanned_output = []
        scanned_output_location = []
        query_output = []
        if event['event_type'].lower() == 'all':
            
            table = boto3.client('dynamodb', region_name=os.environ['table_region'])
            response = table.scan(
            TableName=os.environ['event_table_name'],
            
            FilterExpression='event_epoch_time BETWEEN :a and :b',
            ExpressionAttributeValues = {
                    ":a": {'N': str(from_time)},
                    ":b": {'N': str(to_time)}
                }        
                    )
            logger.info(response)
            logger.info("after response")
            scanned_output.extend(response['Items'])
            while 'LastEvaluatedKey' in response:
                logger.info("in the while loop")
                response = table.scan(
                    TableName=os.environ['event_table_name'],
            
                    FilterExpression='event_epoch_time BETWEEN :a and :b',
                    ExpressionAttributeValues = {
                    ":a": {'N': str(from_time)},
                    ":b": {'N': str(to_time)}
                    },
                    ExclusiveStartKey= response['LastEvaluatedKey'] 
                    )
                scanned_output.extend(response['Items'])
                
            # for location    
            
            table = boto3.client('dynamodb', region_name=os.environ['table_region'])
            response_loc = table.scan(
            TableName=os.environ['location_table_name'],
            
            FilterExpression='event_epoch_time BETWEEN :a and :b',
            ExpressionAttributeValues = {
                    ":a": {'N': str(from_time)},
                    ":b": {'N': str(to_time)}
                }        
                    )
            logger.info(response_loc)
            logger.info("after response")
            scanned_output_location.extend(response_loc['Items'])
            while 'LastEvaluatedKey' in response:
                logger.info("in the while loop")
                response_loc = table.scan(
                    TableName=os.environ['location_table_name'],
            
                    FilterExpression='event_epoch_time BETWEEN :a and :b',
                    ExpressionAttributeValues = {
                    ":a": {'N': str(from_time)},
                    ":b": {'N': str(to_time)}
                    },
                    ExclusiveStartKey= response_loc['LastEvaluatedKey'] 
                    )
                scanned_output_location.extend(response_loc['Items'])
            
                
         
            booln = False
            
        
        else:
            
            query_output_location = []
            response = dynamodb_table.query(
                IndexName='class1-index',
                KeyConditionExpression=Key('class1').eq(event['event_type']),
                
                FilterExpression='event_epoch_time BETWEEN :a and :b',
                ExpressionAttributeValues = {
                    ":a": from_time,
                    ":b": to_time
                }
            )
            query_output.extend(response['Items'])
            while 'LastEvaluatedKey' in response:
                response = dynamodb_table.query(
                    IndexName='class1-index',
                    KeyConditionExpression=Key('class1').eq(event['event_type']),
                
                    FilterExpression='event_epoch_time BETWEEN :a and :b',
                    ExpressionAttributeValues = {
                    ":a": from_time,
                    ":b": to_time
                    },
                    ExclusiveStartKey= response['LastEvaluatedKey'] 
                )
                query_output.extend(response['Items'])
                
            # For location
            
            response_loc = dynamodb_table_location.query(
                IndexName='class1-index',
                KeyConditionExpression=Key('class1').eq(event['event_type']),
                
                FilterExpression='event_epoch_time BETWEEN :a and :b',
                ExpressionAttributeValues = {
                    ":a": from_time,
                    ":b": to_time
                }
                    )
            query_output_location.extend(response_loc['Items'])
            while 'LastEvaluatedKey' in response:
                response_loc = dynamodb_table.query(
                    IndexName='class1-index',
                    KeyConditionExpression=Key('class1').eq(event['event_type']),
                
                    FilterExpression='event_epoch_time BETWEEN :a and :b',
                    ExpressionAttributeValues = {
                    ":a": from_time,
                    ":b": to_time
                    },
                    ExclusiveStartKey= response_loc['LastEvaluatedKey'] 
                )
                query_output_location.extend(response_loc['Items'])
            
            
            
    except Exception as e:
        print("In the exception of insert lat long")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    output = {'markers': []}
    if len(scanned_output) == 0 and len(query_output) == 0:
        dictt = {
                        "markers": [
                           
                        ],
                        "table_data": [
                        
                        ]
                    } 
 

        return dictt
    
    if booln is False:
        table_data = []
        headlines = []
        for i in scanned_output:
            ev_id = i['event_id']['S']
            impacted_supplier_count = 0
            for loc_i in scanned_output_location:
                if ev_id == loc_i['event_id']['S']:
                    
                    impacted_supplier_count += len(loc_i['high_impacted_supplier']['L']) + len(loc_i['medium_impacted_supplier']['L']) + len(loc_i['low_impacted_supplier']['L'])
                    
            
            table_data_dictt = {}
            table_data_location = ""
            
            for j in i['impacted_locations']['L']:
                
                dictt = {}
                if 'NULL'  not in j['M']['city'] and 'none' not in j['M']['city']['S']:
                    location = j['M']['city']['S']
                elif 'NULL' not in j['M']['state'] and 'none' not in j['M']['state']['S']:
                    location = j['M']['state']['S']
                else:
                    location = j['M']['Country']['S']
                
                
                if location != 'none' and i['class1']['S'].lower().strip() != 'default':
                    table_data_location += location+", "
                    dictt.update({'lat': transform(j['M']['lat_long']['M']['lat']['S'], 'lat'), 'lng': transform(j['M']['lat_long']['M']['long']['S'], 'lng'),
                      'Location': location, 'type': i['class1']['S'], "severity": i["severity"]['S'],'Event Id': i['event_id']['S']})
                    
                    filter_severity_marker_data(event,dictt,output)
            if len(table_data_location) > 0:
                table_data_location = table_data_location.rstrip(', ')
                table_data_dictt.update({'Event Id': i['event_id']['S'],"summary":i["summary"]['S'], "severity": i["severity"]['S'],
                                  "location": table_data_location.rstrip(), 
                                  "headline": i['headline']['S'],
                                   "event_date": i['event_date']['S'][:19],
                                   "keywords": ", ".join(key['S'].replace("'",'').lstrip().capitalize() for key in i['tags']['L']),
                                   "impacted_suppliers": impacted_supplier_count
                    })
                if i['headline']['S'] not in headlines:
                    headlines.append(i['headline']['S'])
                    filter_severity_table_data(event,table_data_dictt,table_data)
                
        output.update({'table_data': table_data})
        return output
    else:
        print(response)
        table_data = []
        logger.info(query_output)
        logger.info("in between")
        logger.info(query_output_location)
        headlines = []
        for i in query_output:
            
            ev_id = i['event_id']
            impacted_supplier_count = 0
            for loc_i in query_output_location:
                if ev_id == loc_i['event_id']:
                    
                    impacted_supplier_count += len(loc_i['high_impacted_supplier']) + len(loc_i['medium_impacted_supplier']) + len(loc_i['low_impacted_supplier'])
                    
            
            table_data_dictt = {}
            table_data_location = ""
            for j in i['impacted_locations']:
                dictt = {}
               
                if 'NULL' not in j['city'] and 'none' not in j['city']:
                    location = j['city']
                elif 'NULL' not in j['state'] and 'none' not in j['state']:
                    location = j['state']
                else:
                    location = j['Country']
                
                if location != 'none':
                    table_data_location += location+", "
                    dictt.update({'lat':transform(j['lat_long']['lat'], 'lat'), 'lng': transform(j['lat_long']['long'], 'lng'),'Location': location,
                      'type': i['class1'] , "severity": i["severity"],'Event Id': i['event_id']})
                
                    filter_severity_marker_data(event,dictt,output)
            if len(table_data_location) > 0:
                table_data_location = table_data_location.rstrip(', ')
                table_data_dictt.update({'Event Id': i['event_id'],"summary":i["summary"], "severity": i["severity"],
                                   "headline": i['headline'],
                                   "event_date": i['event_date'][:19],
                                  "location": table_data_location.rstrip(), 
                                  "impacted_suppliers": impacted_supplier_count,
                                  "keywords": ", ".join(key.replace("'",'').lstrip().capitalize() for key in i['tags'])
                    })
                if i['headline'] not in headlines:
                    headlines.append(i['headline'])
                    filter_severity_table_data(event,table_data_dictt,table_data)
        output.update({'table_data': table_data})
        return output
                     
   
