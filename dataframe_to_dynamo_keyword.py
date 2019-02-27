import pandas as pd
import boto3
#import ec2_to_sns as alert
import logging
import ast
import datetime
import json
import re
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
    table_scan = boto3.client('dynamodb', region_name='us-east-1')
    response = table_scan.scan(
            TableName='neo_app_sense_keyword_match_evnts',
            AttributesToGet=[
                'event_id',
                ],        
                    )
    
    print(response)
    current_count = response['Count']
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
            table_key.update({primary_key: str(current_count),
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
            keyword_match = {}
            metal = row['Metals']
            precious_metal = row['Precious Metals']
            currency_code = row['Currency']
            energy = row['Energy']
            polymers = row['Polymers']
            agriculture_produce = row['Agriculture Produce']
            if metal != 0:
                metal_list = fetch_list_frm_string(metal)
                keyword_match.update({'metal': metal_list})
            if precious_metal != 0:
                precious_metal_list = fetch_list_frm_string(precious_metal)
                keyword_match.update({'precious_metal': precious_metal_list})
            if currency_code != 0:
                currency_code_list = fetch_list_frm_string(currency_code)
                keyword_match.update({'currency_code': currency_code_list})
            if energy != 0:
                energy_list = fetch_list_frm_string(energy)
                keyword_match.update({'energy': energy_list})
            if polymers != 0:
                polymers_list = fetch_list_frm_string(polymers)
                keyword_match.update({'polymers': polymers_list})
            if agriculture_produce != 0:
                agriculture_produce_list = fetch_list_frm_string(agriculture_produce)
                keyword_match.update({'agriculture_produce': agriculture_produce_list})

            for i in range(len(rows)):
                irow = rows.iloc[i]
                location = {}
                lat_long = {}
                lat_long.update({'lat': str(irow['Latitude']), 'long': str(irow['Longitude'])})
                location.update({'city': irow['city_name'], 'state': irow['sub_country'], 
                        'lat_long': lat_long,'Country': irow['country']})
                locations.append(location)
                
            dictt.update({'impacted_locations': locations,
                        'keyword_matches': keyword_match
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
    s3_result = s3_conn.list_objects(Bucket='neo-apps-procoure.ai', Prefix = 'sense_eo_output/')
    s3 = boto3.resource('s3')
    files = s3_result['Contents']
    all_data = pd.DataFrame()
    counter = 0
    for i in files:
        if 'xlsx' in i ['Key']:
            local_file_name = i['Key'].split('/')
            s3.Bucket('neo-apps-procoure.ai').download_file(i['Key'], local_file_name[1])
            df = pd.read_excel(local_file_name[1])
            all_data = all_data.append(df)

    #df = pd.read_excel(r"C:\Users\thulasiram.k\Documents\keyword_test.xlsx")
    dynamo_writing('event_id', df, 'neo_app_sense_keyword_match_evnts')
    
   
