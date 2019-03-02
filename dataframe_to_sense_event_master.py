
import pandas as pd
import boto3
#import ec2_to_sns as alert
import logging
import ec2_to_sns as alert
import ast
import sys
import datetime
import json
from datetime import datetime as dtt
#from settings_folder import super_settings as ss
#data = pd.read_excel('.csv',error_bad_lines = False, encoding = "ISO-8859-1")

exp_attribute_values = {}

#data['Date_time']=data['Datetime']

def fetch_list_frm_string(liststring):
    out = ast.literal_eval(liststring)
    return out



def dynamo_writing(primary_key,data,table):
    #data['event_location']=data['Location']
    data = data.rename(columns = {'Date of article':'publication date','URL of News Headline':'url',
                                 'Probability1':'probability1','Probability2': 'probability2',
                                     'Headline of the News':'headline','Location': 'event_location'})
    table = boto3.resource('dynamodb', region_name='us-east-1').Table(table)  
    # table_scan = boto3.client('dynamodb', region_name='us-east-1')
    # response = table_scan.scan(
    #         TableName='neo_app_sense_location_match_events',
    #         AttributesToGet=[
    #             'event_id',
    #             ],        
    #                 )
    
    # print(response)
    # current_count = response['Count']
    current_count = 1
    unique_scrap_id = []
    for i in range(len(data)):
       
        
        row = data.iloc[i]
        current_scrap_id = row['Scrape_id']
        if current_scrap_id not in unique_scrap_id:
            print(current_count)
            print("After count")
           
            dictt = {}
            table_key = {}
            current_count += 1
            unique_scrap_id.append(current_scrap_id)
            rows = data.loc[data['Scrape_id'] == current_scrap_id]
            row = rows.iloc[0]
            print("After row")
            article_url = row['url']
            table_key.update({primary_key: str(current_scrap_id),
                   'article_url': article_url})
            
            epoch_time = int((dtt.strptime(str(row['publication date']), "%Y-%m-%d %H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
            
            tags = row['Tags']
            tags = tags.split(',')
            tags = [x for x in tags if x]
           
            dictt.update({'article_source': str(row['Source']),
                        'class1': str(row['Class Level1']),
                            'class2': str(row['Class Level2']),
                            'content': str(row['Summary']),
                            'epoch_time': epoch_time,
                            'event_date': str(row['Event Date']),
                            'feature': str(row['Feature']),
                            'headline': str(row['headline']),
                            'probability1': str(row['probability1']),
                            'probability2': str(row['probability2']),
                            'publication_date': str(row['publication date']),
                            'severity': str(row['Severity']),
                            'summary': str(row['Summary']),
                            
                            'tags':tags,
                })

            
            
            locations = []
            for i in range(len(rows)):
                irow = rows.iloc[i]
                location = {}
                lat_long = {}
                lat_long.update({'lat': str(irow['Latitude']), 'long': str(irow['Longitude'])})
                location.update({'city': irow['city_name'], 'state': irow['sub_country'], 
                        'lat_long': lat_long,'Country': irow['country']})
                locations.append(location)
                
            dictt.update({'impacted_locations': locations
                        })
            print(dictt)
            print("After dictt")
            
            update_exp = ""
            for key, value in dictt.items():
                print('in the for')
                print(key)
                print(value)
                update_exp += key + ' = :' + key + ','
                key_placeholder = ':' + str(key)
                exp_attribute_values[key_placeholder] = dictt[key]
            update_exp = "SET " + update_exp.rstrip(",")
            response = table.update_item(Key=table_key,
                                        UpdateExpression=update_exp,
                                        ExpressionAttributeValues=exp_attribute_values,
                                        ReturnValues="UPDATED_NEW")
        
            response = response['ResponseMetadata']['HTTPStatusCode']
            
            # if(response==200):
            #     print("inside the sending mail")
            
            #     print("condition met")
            #     response=alert.data_send_via_mail(row)
            #     if(response ==True):
            #         logging.info("mail triggered for the data :::::" +str(row.values))

if __name__ == '__main__':
    
    s3_conn = boto3.client('s3')
    s3_result = s3_conn.list_objects(Bucket='neo-apps-procoure.ai', Prefix = 'sense_event_master_output/')
    s3 = boto3.resource('s3')
    files = s3_result['Contents']
    all_data = pd.DataFrame()
    counter = 0
    for i in files:
        if 'xlsx' in i ['Key']:
            local_file_name = i['Key'].split('/')
            print(local_file_name)
            print("After local file name")
            bucket_address = 'sense_event_master_output/'+local_file_name[1]
            object_acl = s3.ObjectAcl('neo-apps-procoure.ai',bucket_address)
            object_acl.put(ACL='public-read')
            url = "https://s3.amazonaws.com/neo-apps-procoure.ai/sense_event_master_output/"+local_file_name[1]
           
            s3.Bucket('neo-apps-procoure.ai').download_file(i['Key'], local_file_name[1])
            df = pd.read_excel(local_file_name[1])
            all_data = all_data.append(df)

    #df = pd.read_excel(r"C:\Users\thulasiram.k\Documents\test.xlsx")
    dynamo_writing('event_id', df, 'neo_app_sense_event_master')
    try:
        msg = '''Hi All,
                Refresh Match Events for Date: ''' + str(datetime.datetime.now().date()) + '''<alert date> have been generated and placed at below location for your reference.
                '''+url+'''
               Place the above link in browser and the file will download

                thanks and regards
                Sense.AI
                Bristlecone Team'''
        print(msg)
        print("After msg")
        topic = 'arn:aws:sns:us-east-1:356832206364:sense_ai_alerts'
        client = boto3.client('sns', region_name = 'us-east-1')
        subject = 'Senseai Alert'
        response = client.publish(TopicArn=topic,Message=msg,Subject=subject)
        #alert.data_send_via_mail(msg, topic, subject[0:100])
    except Exception as e:
        print("in the exception")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
               
    
   
