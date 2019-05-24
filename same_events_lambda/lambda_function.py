import pandas as pd
import datetime
import json
import re
import itertools
from fuzzywuzzy import fuzz
import ast

def tag_sim(list_tags2, tag):
    list_c = []
    list_d = []
    score2 = []
    for c, d in itertools.combinations(list_tags2, 2):
        if c == tag or d == tag:
            list_c.append(c)
            list_d.append(d)
            score2.append(fuzz.token_set_ratio(c, d))
    data = pd.DataFrame({"Tag1":list_c,"Tag2":list_d,"Score":score2}).sort_values(by = ["Score"], ascending=False).reset_index(drop=True)
    return data

def tag_algo(data, data_id):
    data_loc = []
    for _,row in data.iterrows():
        loc = ast.literal_eval(row["tags (L)"])
        data_loc.append([j["S"] for j in loc])
    data["Tags"] = data_loc
    tag = data[data["event_id (S)"] == data_id]["Tags"][0]
    data_ = tag_sim(data["Tags"], tag)
    array = [data_["Tag2"][0], data_["Tag2"][1], data_["Tag2"][2]]
    df =pd.DataFrame()
    for j in array:
        for _,i in data.iterrows():
            if j == i["Tags"]:
                df = df.append(i)
    del df["Tags"]
    return df



def class_function(df):
    filename = r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/class2/' + str(df["event_Type"]).replace(" ",
                                                                                                                  "_") + ".xlsx"
    df = pd.read_excel(filename)
    return df


def severity_fuction(df):
    filename = r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/Severity/Severity_' + str(
        df["severity"]) + ".xlsx"
    df = pd.read_excel(filename)
    return df


def location_function(df):
    impacted_lat_long = df['lat_long']
    if (float(impacted_lat_long[0]['lat']) > 0):
        lat = 'N'
    else:
        lat = 'S'
    if (float(impacted_lat_long[0]['lng']) > 0):
        long = 'E'
    else:
        long = 'W'
    lat_long = str(lat) + str(long)
    filename = r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/class2/' + lat_long + ".xlsx"
    loc_df = pd.read_excel(filename)
    return loc_df


def entire_change(df, col_name):
    var1 = df[col_name].iloc[10]
    var2 = df[col_name].iloc[-10]
    change = ((var2 - var1) / var1) * 100
    return change

def find_data(event_date, data_rows=8):
    total_data_rows = data_rows+20
    df = pd.read_excel(r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/EnergyData.xlsx')
    event_date = event_date.split(' ')[0]
    df['pub_date'] = pd.to_datetime(df['pub_date'])
    startdate  = datetime.datetime.strptime(event_date, '%m/%d/%Y')
    ent_date = startdate-datetime.timedelta(days=10)
    df['mask'] = df['pub_date'] > (ent_date)
    df = df[df['mask'] != False]
    df = df.head(total_data_rows)
    col_list = ['m1', 'm2', 'm3', 'm4', 'm5']
    for i in col_list:
        col_name = str(i) + "_entirechange"
        df[col_name] = float(entire_change(df, i))
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
    impct_locations = json.loads(row['impacted_locations (L)'])
    for j in impct_locations:

        table_data_location = ""
        if 'NULL' not in j['M']['city'] and 'none' not in j['M']['city']['S']:
            location = j['M']['city']['S']
        elif 'NULL' not in j['M']['state'] and 'none' not in j['M']['state']['S']:
            location = j['M']['state']['S']
        else:
            location = j['M']['Country']['S']
        if location != 'none':
            table_data_location += location + ","
    return table_data_location


def find(df, severity_bit, class_bit, location_bit):
    result = pd.DataFrame()
    if (severity_bit == 1):
        result = severity_fuction(df)
    if (class_bit == 1):
        class_df = class_function(df)
        if(len(result)>0):
            result = pd.merge(result,
                      class_df,
                      on=["event_id (S)","article_url (S)","Scrape_id (S)","article_source (S)","class1 (S)","class2 (S)","content (S)","epoch_time (N)","event_date (S)","event_epoch_time (N)","feature (S)","headline (S)","impacted_locations (L)","probability1 (S)","probability2 (S)","publication_date (S)","severity (S)","summary (S)","tags (L)"],
                      how='inner')
        else:
            result=class_df

    if (location_bit == 1):
        loc_df = location_function(df)
        if(len(result)>0):
            result = pd.merge(result,
                      class_df,
                      on=["event_id (S)","article_url (S)","Scrape_id (S)","article_source (S)","class1 (S)","class2 (S)","content (S)","epoch_time (N)","event_date (S)","event_epoch_time (N)","feature (S)","headline (S)","impacted_locations (L)","probability1 (S)","probability2 (S)","publication_date (S)","severity (S)","summary (S)","tags (L)"],
                      how='inner')
        else:
            result=loc_df
    col_list = ['m1', 'm2', 'm3', 'm4', 'm5', 'm1c', 'm2c', 'm3c', 'm4c', 'm5c']
    for t in col_list:
        result[t] = 0
    entire_list = ['m1_entirechange', 'm2_entirechange', 'm3_entirechange', 'm4_entirechange', 'm5_entirechange']
    for t in entire_list:
        result[t] = 0
    for i in range(0, len(result)):
        print(result.iloc[i])
        date_ext = result['event_date (S)'].iloc[i]
        event_impact_date = pd.read_csv(r's3://neo-apps-procoure.ai/Raw_Files/data_similar_events/event_dates.csv')

        event_impact_date=event_impact_date[event_impact_date["type"]==result['class2 (S)'].iloc[i]]
        event_impact_date=int(event_impact_date['days'].values[0])
        energy_data = find_data(date_ext,event_impact_date)
        print(energy_data.iloc[1])
        date_list = energy_data['pub_date'].tolist()
        for k in col_list:
            value_list = []
            impac_list = energy_data[k].tolist()
            for j in range(0, len(date_list)):
                data_ = {}
                data_.update({"Date": str(str(date_list[j]).split(' ')[0]), str("oil_price"): float(impac_list[j])})
                # data_='''{"Date":"'''+str(date_list[j])+'''","'''+str(k)+'''":"'''+str(impac_list[j])+'''"}'''

                value_list.append(data_)

            result[k].iloc[i] = json.dumps(value_list)
        for f in entire_list:
            result[f].iloc[i] = energy_data[f].iloc[1]
    result = result[result['event_id (S)'] != df["event_Id"]]
    #result=tag_algo(result,df["event_Id"])
    return result , event_impact_date,date_ext


def lambda_handler(event, context):
    data,event_impact_date,start_date = find(event,int(event['severity_bit']),int(event['event_type_bit']), int(event['location_bit']))
    data = data.head(3)
    final_output = {"data": []}
    for i in range(len(data)):
        row = data.iloc[i]
        dictt = {}
        d1=json.loads(row['m2'])
        dd=d1[10]
        print(d1[10])
        d1=str(dd).replace("'",'"').split('"')[3]
        print(d1)
        dictt.update({"event_Id": row["event_id (S)"], "event_Type": row["class2 (S)"],
                      "severity": str(row["severity (S)"]), "headline": row['headline (S)'],
                      "summary": row['summary (S)'], "keywords": return_tags(row), "location": return_location(row),
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
                      "m1_entirechange": str(row['m1_entirechange']),
                      "start_date": str(d1),
                      "no_of_days" : str(event_impact_date)
                      })

        final_output["data"].append(dictt)

    return final_output



