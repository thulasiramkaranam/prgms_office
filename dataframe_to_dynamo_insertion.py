import pandas as pd
import boto3
import ec2_to_sns as alert
import logging
import ast
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
    table_scan = boto3.client('dynamodb', region_name='us-east-1')
    response = table_scan.scan(
            TableName='neo_app_sense_location_match_evnts',
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
            table_key.update({primary_key: str(current_count), 'article_url': article_url})
            
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
            low_impacted_ou = []
            medium_impacted_ou  = []
            high_impacted_ou = []
            low_impacted_supplier = []
            medium_impacted_supplier  = []
            high_impacted_supplier = []
            low_impacted_dc = []
            medium_impacted_dc = []
            high_impacted_dc = []
            entities_impacted = 0
            for i in range(len(rows)):
                irow = rows.iloc[i]
                location = {}
                lat_long = {}
                lat_long.update({'lat': str(irow['Latitude']), 'long': str(irow['Longitude'])})
                location.update({'city': irow['city_name'], 'state': irow['sub_country'], 
                        'lat_long': lat_long,'Country': irow['country']})
                locations.append(location)
                print(irow['impact_level_on_OU'])
                print(type(irow['impact_level_on_OU']))
                
                print("After i row impact")
                i_dc = fetch_list_frm_string(irow['impacted_DC'])
                i_ou = fetch_list_frm_string(irow['impacted_OU'])
                i_su = fetch_list_frm_string(irow['impacted_supplier'])
                e_dc = fetch_list_frm_string(irow['impact_level_on_DC'])
                e_ou = fetch_list_frm_string(irow['impact_level_on_OU'])
                e_su = fetch_list_frm_string(irow['impact_level_on_supplier'])

                for i in range(len(i_ou)):
                    impact_dc = i_ou[i]
                    dc_effect = e_ou[i]
                    print(i_ou)
                    print(e_ou)
                    print("after i and dc")
                    if dc_effect.lower() == 'medium':
                        medium_impacted_ou.append(impact_dc)
                    elif dc_effect.lower() == 'high':
                        high_impacted_ou.append(impact_dc)
                    elif dc_effect.lower() == 'low':
                        low_impacted_ou.append(impact_dc)

                for i in range(len(i_dc)):
                    impact_dc = i_dc[i]
                    dc_effect = e_dc[i]
                   
                    print("after i and dc")
                    if dc_effect.lower() == 'medium':
                        medium_impacted_dc.append(impact_dc)
                    elif dc_effect.lower() == 'high':
                        high_impacted_dc.append(impact_dc)
                    elif dc_effect.lower() == 'low':
                        low_impacted_dc.append(impact_dc)

                for i in range(len(i_su)):
                    impact_dc = i_su[i]
                    dc_effect = e_su[i]
                    print(i_ou)
                    print(e_ou)
                    print("after i and dc")
                    if dc_effect.lower() == 'medium':
                        medium_impacted_supplier.append(impact_dc)
                    elif dc_effect.lower() == 'high':
                        high_impacted_supplier.append(impact_dc)
                    elif dc_effect.lower() == 'low':
                        low_impacted_supplier.append(impact_dc)
            total_impacted =  low_impacted_ou + medium_impacted_ou +\
                  high_impacted_ou + low_impacted_supplier + medium_impacted_supplier + \
                      high_impacted_supplier +  low_impacted_dc + medium_impacted_dc + high_impacted_dc
            
            total_impacted_count = len(set(total_impacted))
            dictt.update({'impacted_locations': locations,
                        'low_impacted_ou': low_impacted_ou,
                        'medium_impacted_ou': medium_impacted_ou,
                        'high_impacted_ou': high_impacted_ou,
                        'low_impacted_supplier': low_impacted_supplier,
                        'medium_impacted_supplier': medium_impacted_supplier,
                        'high_impacted_supplier': high_impacted_supplier,
                        'low_impacted_dc': low_impacted_dc,
                        'medium_impacted_dc': medium_impacted_dc,
                        'high_impacted_dc': high_impacted_dc,
                        'entities_impacted': total_impacted_count
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
            
            #if(response==200):
            print("inside the sending mail")
            msg=''
            subject=''
            topic=''     
            subject='''SenseAI Alert: Event ID  '''+str(row['Scrape_id'])+"  headline  "+str(row['headline'])
            #msg=str(row)
            topic='''arn:aws:sns:us-east-1:356832206364:senseai_location_match'''
            #response=alert.data_send_via_mail(msg,topic,subject[0:100])
            try:
                msg = ""
                msg = msg + '''\nEvent Id: ''' +str(row['Scrape_id'])
                msg = msg + '''\n\nEvent Headline: '''+str(row['headline'][0:200])
                msg = msg + '''\n\nEvent Summary: '''+str(row['Summary'][0:400])
                msg = msg + '''\n\nEvent Severity: '''+str(row['Severity'])
                msg = msg + '''\n\nEvent Impact: '''+str(row['Class Level1'])+","+str(row['Class Level2'])
                msg = msg + '''\n\nimpacted suppliers: ''' +str((row['impacted_DC']))
                msg = msg + '''\n\nimpacted plants: '''+str((row['impacted_OU']))
                msg = msg + '''\n\nimpacted DC: ''' + str((row['impacted_supplier']))
                msg = msg + '''\n\nImpacted Customers : []'''
                
                
                print(msg)
            except Exception as e:
                print(e)
            finally:
                sup_len=0
                sup_len=sup_len+len((row['impacted_DC']))
                sup_len=sup_len+len((row['impacted_DC']))
                sup_len=sup_len+len((row['impacted_supplier']))
                sup_len=sup_len+len((row['impacted_OU']))
                #topic='''arn:aws:sns:us-east-1:356832206364:senseai_location_match'''
                if(sup_len>8):
                    response=alert.data_send_via_mail(msg,topic,subject[0:100])
                #    if(response ==True):
                #        logging.info("mail triggered for the data :::::" +str(row))

if __name__ == '__main__':
    
    s3_conn = boto3.client('s3')
    s3_result = s3_conn.list_objects(Bucket='neo-apps-procoure.ai', Prefix = 'sense_ds_output/')
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

    #df = pd.read_excel(r"C:\Users\thulasiram.k\Documents\test.xlsx")
    dynamo_writing('event_id', df, 'neo_app_sense_location_match_evnts')
    
   
