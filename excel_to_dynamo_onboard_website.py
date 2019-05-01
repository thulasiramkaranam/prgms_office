

import pandas as pd
import boto3
data = pd.read_excel(r"C:\Users\thulasiram.k\prgms_office\EO_new_sources.xlsx")
table = boto3.resource('dynamodb', region_name='us-east-1').Table('Neoapp_sense_crawler')
counter = 0
for i in range(len(data)):
    dictt = {}
    counter += 1
    df_row = data.iloc[i]
    table_key = {}
    table_key.update({'Website': str(df_row['sources'].strip())})
 
    dictt.update({"project_code": df_row['projectcode'], "project_name": "Generic Test",
             "requestor": "vaibhav", "Scrapable": True, "frequency": "24"} )
    update_exp = ""
    exp_attribute_values = {}
    for key, value in dictt.items():

        update_exp +=  key + ' = :' + key + ','
        key_placeholder  = ':' + str(key)
        exp_attribute_values[key_placeholder] = dictt[key]
    update_exp = "SET " + update_exp.rstrip(",")
    response = table.update_item(Key=table_key,
                    UpdateExpression=update_exp,
                    ExpressionAttributeValues=exp_attribute_values,
                    ReturnValues="UPDATED_NEW")
