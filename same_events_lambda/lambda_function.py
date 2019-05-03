import pandas as pd
import datetime
import json
import re
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

def entire_change(df,col_name):
    var1=df[col_name].iloc[0]
    var2=df[col_name].iloc[-1]
    change = ((var2-var1)/var1)*100
    return change

def find_data(event_date,data_rows=8):
    df = pd.read_excel(r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/EnergyData.xlsx',sheet_name='mock_data')

    event_date=event_date.split(' ')[0]

    df['pub_date'] = pd.to_datetime(df['pub_date'])
    ent_date=datetime.datetime.strptime(event_date,'%m/%d/%Y')
    df['mask']=df['pub_date']>(ent_date)
    df=df[df['mask']!=False]
    df=df.head(data_rows)
    col_list = ['m1','m2','m3','m4','m5']
    for i in col_list:
        col_name = str(i)+"_entirechange"
        df[col_name]=float(entire_change(df,i))
    del df['mask']
    return df
   
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
    col_list = ['m1', 'm2', 'm3', 'm4', 'm5','m1c','m2c','m3c','m4c','m5c']
    for t in col_list:
        result[t] = 0
    entire_list=['m1_entirechange','m2_entirechange','m3_entirechange','m4_entirechange','m5_entirechange']
    for t in entire_list:
        result[t] = 0
    for i in range(0,len(result)):
        date_ext=result['event_date (S)'].iloc[i]
        energy_data=find_data(date_ext)
        print(energy_data.iloc[1])
        date_list=energy_data['pub_date'].tolist()
        for k in col_list:
            value_list = []
            impac_list=energy_data[k].tolist()
            for j in range (0,len(date_list)):
                data_ = {}
                data_.update({"Date": str(date_list[j]), str(k): str(impac_list[j])})
                #data_='''{"Date":"'''+str(date_list[j])+'''","'''+str(k)+'''":"'''+str(impac_list[j])+'''"}'''
                
                value_list.append(data_)
            
            result[k].iloc[i]=json.dumps(value_list)
        for f in entire_list:
            
            result[f].iloc[i]=energy_data[f].iloc[1]
    result=result[result['event_id (S)']!=df["event_Id"]]

    return result



def lambda_handler(event, context):
    data = find(event,1,1,1)
    data = data.head(3)
    final_output = {"data": []}
    for i in range(len(data)):
        row = data.iloc[i]
        dictt = {}
        dictt.update({"event_Id": row["event_id (S)"], "event_Type": row["class2 (S)_x"],
        "severity": str(row["severity (S)_x"]),"headline": row['headline (S)_x'],
        "summary": row['summary (S)_x'], "keywords": return_tags(row), "location": return_location(row),
         "m1": json.loads(row['m1']),
         "m2": json.loads(row['m2']),
         "m3": json.loads(row['m3']),
         "m4": json.loads(row['m4']),
         "m5": json.loads(row['m5']),
         "m1c": json.loads(row['m1c']),
         "m2c": json.loads(row['m2c']),
         "m3c": json.loads(row['m3c']),
         "m4c": json.loads(row['m4c']),
         "m5c": json.loads(row['m5c']),
         "m2_entirechange": str(row['m2_entirechange']),
         "m3_entirechange": str(row['m3_entirechange']),
         "m4_entirechange": str(row['m4_entirechange']),
         "m5_entirechange": str(row['m5_entirechange']),
         "m1_entirechange": str(row['m1_entirechange'])

         })
        
        final_output["data"].append(dictt)
        
    return final_output
