

import os
import time
import logging
import boto3
import pandas as pd
import json
import datetime
import requests
import re
from datetime import datetime as dtt  

logger = logging.getLogger()
logger.setLevel(logging.INFO)
mapping = {"Headline": "Headline of the News", "Tags": "Tags",
            "Summary": "Summary", "Class": "Class Level1", "SubClass": "Class Level2", "Location": "Impacted_locations" }
def frame_message(alert_list):
    api_input_list = []
    for alert in alert_list:
        row = alert['df']
        api_input_record = {}
        

        summary = str(row['Summary']).split(".")[0]+'.'
        
        subject = '''Sense.ai Event Alert | Severity '''+str(row['Severity'])+ ''' | ''' + str(row['Headline of the News'])
        msg = '''Subject: ''' +subject + '''\n\n'''
        msg = '''Hi {},'''.format(alert['user'])
        msg = msg + '''\n\nEvent Type\n '''+str(row['Class Level1'])+","+str(row['Class Level2'])
        
        msg = msg + '''\n\nSummary\n '''+str(row['Headline of the News']) + " : " + summary
        
        msg = msg + '''\n\n '''+ str(alert['string'])
        api_input_record.update({"msg": msg, "email": alert['mail_id'], "subject": subject})
        api_input_list.append(api_input_record)

    smtp_mail_url = 'http://172.17.8.82:5000/invoke-smtp'
    output = requests.post(url = smtp_mail_url, data = json.dumps(api_input_list))
    print("invoked api")
    print(output)
    return "invoked api"
        

def lambda_handler(event, context):

    logger.info(event)
    s3 = boto3.resource('s3')
    filename = 'sense_refresh_bc/' + str(dtt.now())[:10]+"refresh.xlsx"
    #s3.Bucket('neo-apps-procoure.ai').download_file(filename, r'C:\Users\thulasiram.k\prgms_office\s3.xlsx')
    
    s3.Bucket('us-east-1-neo-app-senseai').download_file(filename, '/tmp/test.xlsx') 
    df = pd.read_excel('/tmp/test.xlsx')
    #df = pd.read_excel(r"C:\Users\thulasiram.k\prgms_office\s3.xlsx")
    
    alert_list = []
    for record in event['Records']:
        dictt = {}
        if 'NewImage' in record['dynamodb']:
            user = record['dynamodb']['NewImage']['requestor']['S']
            email_id = record['dynamodb']['NewImage']['email_id']['S']
            if 'NewImage' in record['dynamodb']:
                dictt.update({"email_id": record['dynamodb']['NewImage']['email_id']['S'],
                    "comparision_values":  record['dynamodb']['NewImage']['comparision_values']['S'],
                    "comparator": record['dynamodb']['NewImage']['comparator']['S'],
                    "event_attributes": [ i['S'] for i in record['dynamodb']['NewImage']['event_attributes']['L']],
                    "sources": [i['S'] for i in record['dynamodb']['NewImage']['sources']['L']]
                    
                })


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
                            
                            
                            if (str(dictt['comparision_values']).lower()) in str(df[item].iloc[i]).lower():
                                print("in the if conditioin")
                                key_word_list.append(item)
                                df['found'].iloc[i] = 'True'
                                stringg = stringg +", " + item
                                booln = True
                    if booln is True:
                        contains_dictt = {}
                        contains_dictt.update({"df": df.iloc[i], "user": user, "string": stringg,
                                "mail_id": email_id})
                        alert_list.append(contains_dictt)
                        #frame_message(df.iloc[i], user, stringg)
                            
                        
            if('not_contains' in str(dictt['comparator']) or 'not_equals' in str(dictt['comparator'])):

                for i in range(0,len(df)):
                    key_word_list=[]
                    stringg = "Keyword not found in "
                    booln = False
                    for item in event_attributes_list:
                        item = mapping[item]
                        if str(df['Source'].iloc[i]) in dictt['sources']:
                            if (str(dictt['comparision_values']).lower()) not in str(df[item].iloc[i]).lower():
                                
                                key_word_list.append(item)
                                df['found'].iloc[i] = 'True'
                                stringg = stringg + ", " + item
                                booln = True
                    if booln is True:
                        not_contains_dictt = {}
                        not_contains_dictt.update({"df": df.iloc[i], "user": user, "string": stringg,
                                "mail_id": email_id})
                        alert_list.append(not_contains_dictt)
                        #frame_message(df.iloc[i], user, stringg)

          

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
                                if(re.fullmatch(pattern=str(dictt['comparision_values']).lower(), string=str(df[item].iloc[i]).lower() )!=None):
                                    key_word_list.append(item)
                                    df['found'].iloc[i] = 'True'
                                    stringg = stringg + ", " + item
                                    equals_dictt = {}
                                    equals_dictt.update({"df": df.iloc[i], "user": user,
                                        "mail_id": email_id,  "string": stringg})
                                    alert_list.append(equals_dictt)
                                    
    output = frame_message(alert_list)   
    return output             



