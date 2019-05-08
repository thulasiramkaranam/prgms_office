

import boto3
import json
import pandas as pd
"""
final_data = []
table = boto3.client('dynamodb', region_name = 'us-east-1')
response = table.scan(
        TableName= 'neo_app_sense_event_master'
                
                )
#rows = response['Items']
final_data.extend(response['Items'])
mapping = {}
while 'LastEvaluatedKey' in response:
    
    response = table.scan(
            TableName= 'neo_app_sense_event_master',
            ExclusiveStartKey=response['LastEvaluatedKey']
                )
    final_data.extend(response['Items'])
for i in final_data:
    mapping.update({i['event_id']['S']: i['article_url']['S']})
"""

f = open(r"C:\Users\thulasiram.k\prgms_office\mapping.json", 'r')

mapping_data = json.loads(f.read())

table = boto3.resource('dynamodb', region_name='us-east-1').Table('neo_app_sense_event_master') 
data =  pd.read_excel(r'C:\Users\thulasiram.k\Downloads\Sense_data_modified_headlineV5.0.xlsx')
for i in range(len(data)):
       

    row = data.iloc[i]
    print(row)
    dictt = {}
    table_key = {}
    event_id = row['event_Id']
    article_url = mapping_data[event_id]
    table_key.update({'event_id': str(event_id),
            'article_url': article_url})
    
    dictt.update({
                    'headline': str(row['Curated_headline']),
                    'headline_original': str(row['headline'])
        })

    
    
    update_exp = ""
    exp_attribute_values = {}
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

    




