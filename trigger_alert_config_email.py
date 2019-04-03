

import os
import time
import logging
import boto3
import pandas as pd
import datetime
import re
from datetime import datetime as dtt  

logger = logging.getLogger()
logger.setLevel(logging.INFO)
mapping = {"Headline": "Headline of the News", "Tags": "Tags",
            "Summary": "Summary", "Class": "Class Level1", "SubClass": "Class Level2" }

def fetch_configuration_data():
    table = boto3.client('dynamodb', region_name='us-east-1')
    response = table.scan(
            TableName='neoapp_sense_alert_configuration')
  
    users_list = []
    for i in response['Items']:
        usr_dictt = {}

        usr_dictt.update({"email_id": i['email_id']['S'],
                    "comparision_values":  i['comparision_values']['S'],
                    "comparator": i['comparator']['S'],
                    "event_attributes": [j['S'] for j in i['event_attributes']['L']],
                    "sources": [j['S'] for j in i['sources']['L']],
                    "requestor": i['requestor']['S']
                    
                })
        users_list.append(usr_dictt)

    return users_list


def frame_message(row, user, found_in):
    
    client = boto3.client('sns', region_name = 'us-east-1')

    topic = 'arn:aws:sns:us-east-1:356832206364:sense_alert_thulasi'
    subject = '''Sense.ai Event Alert | Severity '''+str(row['Severity'])+ '''|''' + str(row['Headline of the News'])
    
    msg = "Hi {},".format(user)
    msg = msg + '''\n\nEvent Details\n '''+str(row['Class Level1'])+","+str(row['Class Level2'])
    
    msg = msg + '''\n\nEvent Headline\n '''+str(row['Headline of the News'][0:200])
    msg = msg + '''\n\nEvent Summary\n '''+str(row['Summary'][0:400])
    msg = msg + '''\n\n '''+ found_in
    
    response = client.publish(TopicArn=topic,Message=msg,Subject=subject[0:100])
    print("mail sent")

def lambda_handler(dictt, context):

    
    s3 = boto3.resource('s3')
    filename = 'sense_ds_input/sense_output/' + str(dtt.now())[:10]+"refresh.xlsx"
    #s3.Bucket('neo-apps-procoure.ai').download_file(filename, r'C:\Users\thulasiram.k\prgms_office\s3.xlsx')
    user = dictt['requestor']
    s3.Bucket('neo-apps-procoure.ai').download_file(filename, '/tmp/test.xlsx') 
    df = pd.read_excel('/tmp/test.xlsx')
    #df = pd.read_excel(r"C:\Users\thulasiram.k\prgms_office\s3.xlsx")
    

    event_attributes_list=dictt['event_attributes']
           
    
    df['word'] = ''
    df['found']='False'
    
    if('contains' in str(dictt['comparator'])):
        for i in range(0,len(df)):
            key_word_list=[]
            stringg = "Keyword found in "
            booln = False
            for item in event_attributes_list:
                item = mapping[item]
                if str(df['Source'].iloc[i]) in dictt['sources']:
                    
                    
                    if (str(dictt['comparision_values'])) in str(df[item].iloc[i]):
                        print("in the if conditioin")
                        key_word_list.append(item)
                        df['found'].iloc[i] = 'True'
                        stringg = stringg +", " + item
                        booln = True
            if booln is True:

                frame_message(df.iloc[i], user, stringg)
                    
                
    if('not_contains' in str(dictt['comparator']) or 'not_equals' in str(dictt['comparator'])):

        for i in range(0,len(df)):
            key_word_list=[]
            stringg = "Keyword not found in "
            booln = False
            for item in event_attributes_list:
                item = mapping[item]
                if str(df['Source'].iloc[i]) in dictt['sources']:
                    if (str(dictt['comparision_values'])) not in str(df[item].iloc[i]):
                        
                        key_word_list.append(item)
                        df['found'].iloc[i] = 'True'
                        stringg = stringg + ", " + item
                        booln = True
            if booln is True:
                frame_message(df.iloc[i], user, stringg)

    

    if('equals' in str(dictt['comparator'])):
        for i in range(0 ,len(df)):
            key_word_list =[]
            stringg = "Keyword found in "
            booln = False

            for item in event_attributes_list:
                item = mapping[item]
                if str(df['Source'].iloc[i]) in dictt['sources']:
                    for iter_item in item.replace(',','').replace("'",'').split(' '):
                        # result = str(re.fullmatch(pattern=str(json_data['comparision_values']), string=str(df[item].iloc[i]))
                        if(re.fullmatch(pattern=str(dictt['comparision_values']), string=str(df[item].iloc[i]) )!=None):
                            key_word_list.append(item)
                            df['found'].iloc[i] = 'True'
                            stringg = stringg + ", " + item
                            frame_message(df.iloc[i], user, stringg)
                            booln = True
            if booln is True:
                frame_message(df.iloc[i], user, stringg)


cd = fetch_configuration_data()
for i in cd:
    lambda_handler(i,"raj")

