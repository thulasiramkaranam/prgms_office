
import distance as dist
import os
os.chdir('/home/ec2-user/linuxcode/framework')
import redundancy_handling as rh
import keyword_match as keywordSearch
import sniffer as sniff
import merge_same_locations_df as ms
import copy
import driver as crawl_driver
import data_purifier as dp
import event_id
from datascience import  model_predictor_apps as pred
import dataframe_to_dynamo_insertion as dbentry
import get_websites as gw
import delete_duplicates as dd
import time
import os
import ec2_to_s3 as ec22s3
import pandas as pd
from settings_folder import super_settings as ss
import ec2_to_sns as mail
import datetime



"""
web_config=gw.get_website_data()
#print(web_config)
#print("After webconfi")
#calling the crawl function
print("calling crawler")
df=crawl_driver.main(web_config)
#print(df)
#print("After df")
#purifing data
df=dp.main(df)
print("before removing duplicates")
#df = dd.remove_duplicates(df)
df=df.drop_duplicates(subset='Headline of the News', keep="last")
file=str(datetime.datetime.now().date())+'.xlsx'
print(len(df))
os.chdir('/home/ec2-user/linuxcode/framework')
print("before writing")
df.to_excel(file)
print("after writing")
#file = 'test26thfeb.xlsx'
os.chdir('/home/ec2-user/linuxcode/framework')
#file = 'thefinaltest.xlsx'
#df = pd.read_excel(file)
#df=df.head(100)
s3_file_location = ec22s3.upload_file(file,ss.bucket_name,str(ss.scraped_history + file))
msg="Hi,\nScraped file has been uploaded to the location given below in senseai instance,\n Location :\n"+str(s3_file_location)+"\n place the above link in browser and the file will download"
mail.data_send_via_mail(msg)
"""
file=str(datetime.datetime.now().date())+'.xlsx'
os.chdir('/home/ec2-user/linuxcode/framework')
print("scrapping done")
#callg model
df = pd.read_excel(file)
#df=df[df['Class Level1']=='Weather']
print(len(df))
#df = df.head(150)
print("After scrapping")
df=pred.get_feature(df)
df1=copy.deepcopy(df)
print(df1.head(10))
df=pred.main(df)
try:
    wind_file = "/home/ec2-user/linuxcode/framework/testing/wind_data"+str(datetime.datetime.now().date())+".xlsx"
    wind_df=pd.read_excel(wind_file , index=False)
    df=df.append(wind_df)
except Exception as E:
    print("wind_data")
    print(E)

#df=event_id.main(df)
#f1=copy.deepcopy(df)

# removing data with default class 2
try:
    #f = df[df['Class Level2'] != 'Default']
    df1=copy.deepcopy(df)
    df = df[df['Event Date'] != 'No_Date_found']
    df = ms.merge_locations(df)
    ref_file_name = str(datetime.datetime.now().date()) + 'refresh.xlsx'
    df = df.drop_duplicates('Scrape_id', keep='last')
    df = df.drop(columns=['city_name', 'sub_country', 'country', 'Latitude', 'Longitude'])
    try:
        df=rh.rd_handle_main(df)
        df.to_excel(ref_file_name)
    except Exception as e:
        print(e)
        print("inside RH")
    #df1=copy.deepcopy(df)
    predicted_file_location = "https://s3.amazonaws.com/neo-apps-procoure.ai/sense_ds_input/sense_output/" + ref_file_name
    ec22s3.upload_file(ref_file_name, ss.bucket_name, str(ss.sense_team_input + 'sense_output/' + ref_file_name))
    msg = "Hi,\nRefreshed file before manual curation has been uploaded to the location given below,\n Location :\n" + str(
        predicted_file_location) + "\n place the above link in browser and the file will download"
    #mail.data_send_via_mail(msg)
    print("mail sent")
except Exception as E:
    print("inside_refresh")
    print(E)

"""
finally:
    try:
        df = dist.calculate_impact(df)
        print("After calculating impact")
        os.chdir('/home/ec2-user/linuxcode/framework')
        filename = str(datetime.datetime.now().date()) + '_predicted.xlsx'
        # filename = '26thfeb_pred.xlsx'
        print("created the prediction file")
        print(df)
        #print(type(df['impacted_DC']))
        cust_list = ['impacted_DC', 'impacted_OU', 'impacted_OU']
        dd = df.head(0)
        #for elemt in cust_list:
        df1 = df[df['impacted_DC'] != '[]']
        dd = dd.append(df1)
        df1 = df[df['impacted_OU'] != '[]']
        dd = dd.append(df1)
        df1 = df[df['impacted_OU'] != '[]']
        dd = dd.append(df1)
        df = dd.drop_duplicates()
        print(df)

        df.to_excel(filename)
        predicted_file_location = ec22s3.upload_file(filename, ss.bucket_name, str(ss.sense_team_input + filename))
        predicted_file_location = "https://s3.amazonaws.com/neo-apps-procoure.ai/sense_ds_input/"+filename
        ec22s3.upload_file(filename,ss.bucket_name,str(ss.sense_team_input + filename))

        # keywordExtraction code
        msg = "Hi ,\nLocation match file has been uploaded to the location given below,\n Location :\n" + str(
            predicted_file_location) + "\n place the above link in browser and the file will download"
        mail.data_send_via_mail(msg)
        print("mail sent")
    except Exception as E:
        print(E)

    finally:
        df1 = keywordSearch.main(df1)
        df1 = df1[df1['Keyword Found'] == 'Yes']
        os.chdir('/home/ec2-user/linuxcode/framework')
        filename = str(datetime.datetime.now().date()) + 'keyword_pred.xlsx'
        print("After calling keyword")
        df1.to_excel(filename)
        predicted_file_location = "https://s3.amazonaws.com/neo-apps-procoure.ai/sense_ds_input/EO_output/" + filename
        ec22s3.upload_file(filename, ss.bucket_name, str(ss.sense_team_input + 'EO_output/' + filename))

        # dynamo entry and other stuff
        # print("before entering in dynamo")
        # import pandas as pd
        # df = pd.read_excel('predicted.xlsx')

        # dbentry.dynamo_writing(ss.primary_key,ss.columns,df,ss.data_table_name)
        msg = "Hi,Keyword match data has been uploaded to the location given below,\n Location :\n" + str(
           predicted_file_location) + "\n place the above link in browser and the file will download"
        mail.data_send_via_mail(msg)
        print("mail sent")
"""
try:
    df1 = keywordSearch.main(df1)
    df1 = df1[df1['Keyword Found'] == 'Yes']
    os.chdir('/home/ec2-user/linuxcode/framework')
    filename = str(datetime.datetime.now().date()) + 'keyword_pred_test.xlsx'
    print("After calling keyword")
    df1=df1.drop_duplicates(subset='Scrape ID',keep='last')
    df1.to_excel(filename)
    predicted_file_location = "https://s3.amazonaws.com/neo-apps-procoure.ai/sense_ds_input/EO_output/" + filename
    ec22s3.upload_file(filename, ss.bucket_name, str(ss.sense_team_input + 'EO_output/' + filename))

    # dynamo entry and other stuff
    # print("before entering in dynamo")
    # import pandas as pd
    # df = pd.read_excel('predicted.xlsx')

    # dbentry.dynamo_writing(ss.primary_key,ss.columns,df,ss.data_table_name)
    msg = "Hi,Keyword match data has been uploaded to the location given below,\n Location :\n" + str(
           predicted_file_location) + "\n place the above link in browser and the file will download"
    #mail.data_send_via_mail(msg)
    print("mail sent")
except Exception as E:
    print("code is failing",E)
