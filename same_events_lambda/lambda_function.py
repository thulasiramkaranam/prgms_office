import pandas as pd
import re
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def class_function(df):
    filename = r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/class2/' + str(df["event_Type"]).replace(" ","_") + ".xlsx"
    df = pd.read_excel(filename)
    return df
def severity_fuction(df):
    filename = r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/Severity/Severity_' + str(df["severity"]) + ".xlsx"
    df = pd.read_excel(filename)
    return df

def location_function(df):
    impacted_lat_long=df['lat_long']
    if(float(impacted_lat_long[0]['lat'])>0):
        lat='N'
    else:
        lat = 'S'
    if (float(impacted_lat_long[0]['lng']) > 0):
        long = 'E'
    else:
        long = 'W'
    lat_long=str(lat)+str(long)
    filename = r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/class2/' + lat_long + ".xlsx"
    loc_df=pd.read_excel(filename)
    return loc_df

def find(df,severity_bit,class_bit,location_bit):
    if(severity_bit==1):
        sev_df= severity_fuction(df)
    if(class_bit==1):
        class_df=class_function(df)

    result = pd.merge(sev_df,
                      class_df,
                      on='event_id (S)',
                      how='inner')
    if (location_bit==1):
        loc_df = location_function(df)
    result = pd.merge(result,
                      loc_df,
                      on='event_id (S)',
                      how='inner')
    return result
def return_tags(row_element):
    tags = ''
    if 'tags (L)_x' in row_element:
        tag_list = row_element['tags (L)_x']
        counter = 0
        tag_list = json.loads(tag_list)
        for tag in tag_list:
            
            word = " ".join(re.findall("[a-zA-Z]+", tag['S']))
            if len(word) > 3:
                counter += 1
                word = word.capitalize()
                tags = tags + word + ','
            if counter == 5:
                break
        return tags
    else:
        tags = 'no tags available'

def return_location(row):
    
    impct_locations = json.loads(row['impacted_locations (L)_x'])
    print(impct_locations)
    print(type(impct_locations))
    for j in impct_locations:
        
        table_data_location = ""
        if 'NULL'  not in j['M']['city'] and 'none' not in j['M']['city']['S']:
            location = j['M']['city']['S']
        elif 'NULL' not in j['M']['state'] and 'none' not in j['M']['state']['S']:
            location = j['M']['state']['S']
        else:
            location = j['M']['Country']['S']
        if location != 'none':
            table_data_location += location+","
    return table_data_location

def lambda_handler(event, context):
   
    data = find(event,1,1,1)
    data = data.head(5)
    final_output = {"data": []}
    for i in range(len(data)):
        row = data.iloc[i]
        dictt = {}
        dictt.update({"event_Id": row["event_id (S)"], "event_Type": row["class2 (S)_x"],
        "severity": str(row["severity (S)_x"]),"headline": row['headline (S)_x'],
        "summary": row['summary (S)_x'], "keywords": return_tags(row), "location": return_location(row) })
        print(dictt)
        final_output["data"].append(dictt)
    logger.info(final_output)
    logger.info("after final output")
    return final_output