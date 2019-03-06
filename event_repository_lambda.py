

import json
import boto3
import logging
import re
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    output = []
    table = boto3.client('dynamodb', region_name='us-east-1')
    response = table.scan(
            TableName='neo_app_sense_event_master',
                   
                    )
    rows = response['Items']
    for row in rows:
        dictt = {}
        tags = ''
        if 'tags' in row:
            tag_list = row['tags']['L']
            counter = 0
            for tag in tag_list:
                
                word = " ".join(re.findall("[a-zA-Z]+", tag['S']))
                if len(word) > 3:
                    counter += 1
                    tags = tags + word + ','
                if counter == 5:
                    break
        else:
            tags = 'no tags available'
        table_data_location = ""
        for j in row['impacted_locations']['L']:
                
            if 'NULL'  not in j['M']['city'] and 'none' not in j['M']['city']['S']:
                location = j['M']['city']['S']
            elif 'NULL' not in j['M']['state'] and 'none' not in j['M']['state']['S']:
                location = j['M']['state']['S']
            else:
                location = j['M']['Country']['S']
            if location != 'none':
                table_data_location += location+","
        if len(table_data_location) > 0: 
            dictt.update({"event_Id": row['event_id']['S'],
                   "event_Type": row['class1']['S'],
                   "severity": row['severity']['S'],
                    "headline": row['headline']['S'],
                    "summary": row['summary']['S'],
                    "keywords": tags,
                    "location": table_data_location
            
            })
            output.append(dictt)
    final_output = {
              "statusCode": 200,
              "body": output,
              "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
              }
            }
    return output
    logger.info(response)
    logger.info("After response")
    
