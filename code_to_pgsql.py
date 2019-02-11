
import json
import psycopg2
import pandas as pd
import unidecode
def make_conn():
    df = pd.read_excel(r'C:\Users\thulasiram.k\Documents\latlongtill20732to21232.xlsx')
    db_name = 'senseai'
    db_user = 'neo_app_vso_root'
    db_host = 'neo-app-vso.c8yss8lfzn7r.us-east-1.rds.amazonaws.com'
    db_pass = 'Bcone,12345'
    conn = None
    result = []
    #query = "select * from senseai_lat_long"
    query = "INSERT INTO senseai_lat_long VALUES(%s,%s,%s,%s) ON CONFLICT (city_name, sub_country, country) DO NOTHING"

    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, db_user, db_host, db_pass))
    cursor = conn.cursor()
    # for select query
    # cursor.execute(query)
    # result = cursor.fetchone()
    # print(result)
    # print("After result")
    # records = cursor.fetchall() 
    # counter = 0
    # for row in records:
    #     counter += 1
    #     print(row[3])
    #     print("After row")
    #     if counter == 100:
    #         break
    
    # end of select query

     
    for i in range(len(df)):
        row = df.iloc[i]
        if i > 20730 and i <=21230:
            if i == 21732 or i == 21230: 
                print(row)
            if row['lat-long'] != 'Unable to fetch recheck city name':
                city_name = unidecode.unidecode(row['name'].strip())
                country = unidecode.unidecode(row['country'].strip())
                if type(row['subcountry']) != float:
                    sub_country = unidecode.unidecode(row['subcountry'].strip())
                else:
                    sub_country = 'None'
                lat_long = row['lat-long'].strip()
                lat_long = lat_long.replace("'", '"')
                lat_long = lat_long.replace(" ", "")
                lat_long = json.loads(lat_long)
                lat_long = json.dumps(lat_long)
                print("in the final")
                cursor.execute(query, (city_name.lower(),sub_country.lower(),country.lower(),lat_long))
                
            
    conn.commit()
    cursor.close()
 



def lambda_handler(event, context):
    # TODO implement
    
    make_conn()
    
    print("After connectioin object")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
lambda_handler(1,2)


