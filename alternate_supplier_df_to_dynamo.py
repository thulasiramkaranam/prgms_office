

toinsert = {
  "alternate_suppliers": [
    {
      "123": [
        {
          "Item_description": "test description",
          "item_no": "ITEM-345",
          "supplier_id": "345",
          "supplier_name": "uniliver"
        },
        {
          "Item_description": "test description",
          "item_no": "ITEM-444",
          "supplier_id": "444",
          "supplier_name": "godrej"
        },
        {
          "Item_description": "test description",
          "item_no": "ITEM-555",
          "supplier_id": "555",
          "supplier_name": "mahindra"
        }
      ]
    }
  ],
  "event_id": "ED_TN_24189",
  "event_time": 1251167009
}

import json

import boto3
import datetime
from datetime import datetime as dtt
import pandas as pd
df = pd.read_excel(r"C:\Users\thulasiram.k\prgms_office\test2_final.xlsx")
df = df.drop_duplicates(['Alternate Supplier ID','Item ID', 'Impacted Supplier ID','Event ID'],keep= 'last')
unique_events =  df['Event ID'].unique().tolist()
finallist = []
table = boto3.resource('dynamodb', region_name='us-east-1').Table('neoapp_sense_supplier_recommendation')  
for event_id in unique_events:
    event_id_rows = df.loc[df['Event ID'] == event_id]
    table_key = {}
    table_key.update({"event_id": str(event_id)})
    dictt = {}
    alternate_suppliers = []
    for i in range(len(event_id_rows)):
        each_row = event_id_rows.iloc[i]
        event_epoch_time = int((dtt.strptime(str(each_row['Event Date']), "%Y-%m-%d %H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        dictt.update({"event_time": event_epoch_time})
        impactd_suplr_id = str(each_row['Impacted Supplier ID'])
        if len(alternate_suppliers) == 0:
            alt_sup = {}
            alt_sup_lst = []
            alt_sup_dict = {}
            alt_sup_dict.update({"Item_description": str(each_row['Item Description']),
                        "item_no": str(each_row['Item ID']),
                         "supplier_id": str(each_row['Alternate Supplier ID']),
                         "supplier_name": str(each_row["Supplier Name"])})
            alt_sup_lst.append(alt_sup_dict)
            alt_sup.update({str(each_row["Impacted Supplier ID"]): alt_sup_lst})
            alternate_suppliers.append(alt_sup)
        else:
            for ii in range(len(alternate_suppliers)):
                if impactd_suplr_id in alternate_suppliers[ii].keys():
                    int_dictt = {}
                    int_dictt.update({"Item_description": each_row['Item Description'],
                        "item_no": str(each_row['Item ID']),
                         "supplier_id": str(each_row['Alternate Supplier ID']),
                         "supplier_name": str(each_row["Supplier Name"])})
                    alternate_suppliers[ii][impactd_suplr_id].append(int_dictt)
                    break
                else:
                    if len(alternate_suppliers) == ii+1:
                        new_alt_sup = {}
                        new_alt_sup_lst = []
                        new_alt_sup_dict = {}
                        new_alt_sup_dict.update({"Item_description": str(each_row['Item Description']),
                                    "item_no": str(each_row['Item ID']),
                                    "supplier_id": str(each_row['Alternate Supplier ID']),
                                    "supplier_name": str(each_row["Supplier Name"])})
                        new_alt_sup_lst.append(new_alt_sup_dict)
                        new_alt_sup.update({str(each_row["Impacted Supplier ID"]): new_alt_sup_lst})
                        alternate_suppliers.append(new_alt_sup)
    dictt.update({"alternate_suppliers": alternate_suppliers})
    finallist.append(dictt)
    print(dictt)
    print("------------------------------------------------------------------")  
    f = open("text.txt", "w")
    f.write(str(finallist))
    exp_attribute_values = {}
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


