import os
import time
import logging
import boto3
import pandas as pd
import datetime
import re
import json
from datetime import datetime as dtt
#import psycopg2
logger = logging.getLogger()
logger.setLevel(logging.INFO)
mapping = {"Headline": "Headline of the News", "Tags": "Tags",
           "Summary": "Summary", "Class": "Class Level1", "SubClass": "Class Level2",
           "Location": "Impacted_locations" , "Content": "Content of the News"}



def set_alert_log(df, username):
    email_data = {}
    email_data.update({"email_id": username,
                "mailed_date": str(dtt.now())[:10],
                "evnt_date": str(df['Event Date']).split(' ')[0],
                "severity": str(df['Severity']) ,
                "headline":  str(df['Headline of the News']),
                "summary":  str(df['Summary']).replace("'", "''").replace(',', '"') })
    lambda_input = json.dumps(email_data)
    client = boto3.client('lambda')
    client.invoke_async(
        FunctionName= os.environ['lambda_async_name'],
        InvokeArgs= lambda_input
            )
    logger.info("called the lambda to insert in db ")

def forums_frame_message(row, user, found_in,email_id):
    set_alert_log(row,email_id)
    client = boto3.client('sns', region_name = 'us-east-1')
    contents_list = row['Content of the News'].split("|")
    summary = str(row['Summary']).split(".")[0 ] +'.'
    topic = os.environ['sns_topic_url']
    subject = '''Sense.ai Event Alert | Severity ''' +str(row['Severity'] )+ ''' | ''' + str (row['Headline of the News'])

    msg = "Hi {},".format(user)
    msg = msg + '''\n\nEvent Type\n''' +str(row['Class Level1'] ) +", " +str(row['Class Level2'])

    msg = msg+ '''\n\nSummary\n ''' +str(row['Headline of the News']) + " : " + summary
    msg = msg + '''\n\n '''+ found_in
    msg = msg + '''\n\n ''' + "Topic Discussed :" + contents_list[1].strip()
    msg = msg + '''\n ''' + "Number of views :" + contents_list[2].strip()
    msg = msg + '''\n ''' + "Number of comments :" + contents_list[3].strip()
    msg = msg + '''\n ''' + "Sentiment :" + contents_list[0].strip()


    response = client.publish(TopicArn=topic ,Message=msg ,Subject=subject[0:100])
    print("mail sent")


def forums_mails_frame(row, comparasion_values, column_values, user, email_id):
    column_values.append("Content")
    stringg = "Keyword found in "
    comp_val_found = ""
    item_found = ""
    booln = False
    comp_val_listt = []
    item_found_listt = []
    key_word_list = []
    for item in column_values:
        item = mapping[item]
        
        for comp_val in comparasion_values:

            if (str(comp_val).lower()) in str(row[item]).lower():
                print("in the if conditioin")
                key_word_list.append(item)
                
                if comp_val not in comp_val_listt:
                    comp_val_found += str(comp_val) + ", "
                    comp_val_listt.append(comp_val)
                if item not in item_found_listt:
                    item_found += str(item) + ", "
                    item_found_listt.append(item)
                print("forums comparasion satisfied")
                booln = True
    if booln is True:
        print(item_found)
        print("after item found")
        stringg = comp_val_found + stringg + item_found
        forums_frame_message(row, user, stringg, email_id)
            


def frame_message(row, user, found_in,email_id):
    set_alert_log(row,email_id)
    client = boto3.client('sns', region_name = 'us-east-1')
    summary = str(row['Summary']).split(".")[0 ] +'.'
    topic = os.environ['sns_topic_url']
    subject = '''Sense.ai Event Alert | Severity ''' +str(row['Severity'] )+ ''' | ''' + str (row['Headline of the News'])

    msg = "Hi {},".format(user)
    msg = msg + '''\n\nEvent Type\n''' +str(row['Class Level1'] ) +", " +str(row['Class Level2'])

    msg = msg+ '''\n\nSummary\n ''' +str(row['Headline of the News']) + " : " + summary

    msg = msg + '''\n\n '''+ found_in

    response = client.publish(TopicArn=topic ,Message=msg ,Subject=subject[0:100])
    print("mail sent")


def lambda_handler(event, context):

    logger.info(event)
    s3 = boto3.resource('s3')

    s3_conn = boto3.client('s3')
    s3_result = s3_conn.list_objects(Bucket= os.environ['s3_bucketname'], Prefix = os.environ['s3_folder_after_curation'])
    files = s3_result['Contents']
    df = pd.DataFrame()
    for i in files:
        if '.xlsx' in i['Key'] and str(i['LastModified'].date()) == str(dtt.now().date()):
            filename = i['Key']
            s3.Bucket(os.environ['s3_bucketname']).download_file(filename, '/tmp/test.xlsx') 
            df = pd.read_excel('/tmp/test.xlsx')
            print("in the event master output file")
    if len(df) == 0:
        filename = os.environ['s3_folder_before_curation'] + str(dtt.now())[:10]+"refresh.xlsx"
        print("Taking before manual curation file")
        #s3.Bucket('neo-apps-procoure.ai').download_file(filename, r'C:\Users\thulasiram.k\prgms_office\s3.xlsx')
    
        s3.Bucket(os.environ['s3_bucketname']).download_file(filename, '/tmp/test.xlsx') 
        df = pd.read_excel('/tmp/test.xlsx')
        #df = pd.read_excel(r"C:\Users\thulasiram.k\prgms_office\s3.xlsx")
    
    
    for record in event['Records']:
        dictt = {}
        if 'NewImage' in record['dynamodb']:
            user = record['dynamodb']['NewImage']['requestor']['S']
            email_id = record['dynamodb']['NewImage']['email_id']['S']
            comparison_values_list = [ i['S'] for i in record['dynamodb']['NewImage']['comparision_values']['L']]
            if 'NewImage' in record['dynamodb']:
                dictt.update({"email_id": record['dynamodb']['NewImage']['email_id']['S'],
                    "comparision_values": [ i['S'] for i in record['dynamodb']['NewImage']['comparision_values']['L']],
                    "comparator": record['dynamodb']['NewImage']['comparator']['S'],
                    "event_attributes": [ i['S'] for i in record['dynamodb']['NewImage']['event_attributes']['L']],
                    "sources": [i['S'] for i in record['dynamodb']['NewImage']['sources']['L']]
                    
                })


            event_attributes_list=dictt['event_attributes']
           
        
            df['word'] = ''
            df['found']='False'
            
            if 'contains' in str(dictt['comparator']):
                for i in range(0, len(df)):
                    key_word_list = []
                    stringg = "Keyword found in "
                    comp_val_found = ""
                    item_found = ""
                    booln = False
                    comp_val_listt = []
                    if 'https://forums.edmunds.com/discussions/tagged/x/repairs-maintenance' in str(df              ['Source'].iloc[i]):
                        print("in the forums if condition satisfied")
                        forums_mails_frame(df.iloc[i], comparison_values_list, event_attributes_list, user, email_id )

                        

                    else:
                        for item in event_attributes_list:
                            item = mapping[item]

                            if str(df['Source'].iloc[i]) in dictt['sources']:

                                for comp_val in comparison_values_list:

                                    if (str(comp_val).lower()) in str(df[item].iloc[i]).lower():
                                        print("in the if conditioin")
                                        key_word_list.append(item)
                                        df['found'].iloc[i] = 'True'
                                        if comp_val not in comp_val_listt:
                                            comp_val_found += str(comp_val) + ", "
                                            comp_val_listt.append(comp_val)

                                        item_found += str(item) + ", "
                                        
                                        booln = True
                        if booln is True:
                            stringg = comp_val_found + stringg + item_found
                            frame_message(df.iloc[i], user, stringg,email_id)
                                
                        
            if 'not_contains' in str(dictt['comparator']) or 'not_equals' in str(dictt['comparator']):

                for i in range(0,len(df)):
                    key_word_list=[]
                    stringg = "Keyword not found in "
                    booln = False
                    comp_val_found = ""
                    item_found = ""
                    comp_val_listt = []
                    for item in event_attributes_list:
                        item = mapping[item]
                        if str(df['Source'].iloc[i]) in dictt['sources']:

                            for comp_val in comparison_values_list:
                                if (str(comp_val).lower()) not in str(df[item].iloc[i]).lower():

                                    key_word_list.append(item)
                                    df['found'].iloc[i] = 'True'
                                    if comp_val not in comp_val_listt:
                                        comp_val_found += str(comp_val) + ", "
                                        comp_val_listt.append(comp_val)
                                    
                                    item_found += str(item_found) + ", "
                                    #stringg = comp_val + ", " + stringg + ", " + item
                                    booln = True
                    if booln is True:
                        stringg = comp_val_found + stringg + item_found
                        frame_message(df.iloc[i], user, stringg,email_id)

            if 'equals' in str(dictt['comparator']):
                for i in range(0, len(df)):
                    key_word_list = []
                    stringg = "Keyword found in "
                    booln = False
                    comp_val_found = ""
                    item_found = ""
                    comp_val_listt = []
                    for item in event_attributes_list:
                        item = mapping[item]
                        if str(df['Source'].iloc[i]) in dictt['sources']:
                            for iter_item in item.replace(',', '').replace("'", '').split(' '):
                                # result = str(re.fullmatch(pattern=str(json_data['comparision_values']), string=str(df[item].iloc[i]))

                                for comp_val in comparison_values_list:

                                    if (re.fullmatch(pattern=str(comp_val).lower(),string=str(df[item].iloc[i]).lower()) != None):
                                        key_word_list.append(item)
                                        df['found'].iloc[i] = 'True'
                                        if comp_val not in comp_val_listt:
                                            comp_val_found += str(comp_val) + ", "
                                            comp_val_listt.append(comp_val)
                                        
                                        item_found += str(item_found) + ", "
                                        #stringg = comp_val + ", " + stringg + ", " + item
                                        #frame_message(df.iloc[i], user, stringg,email_id)
                                        booln = True
                    if booln is True:
                        stringg = comp_val_found + stringg + item_found
                        frame_message(df.iloc[i], user, stringg,email_id)



