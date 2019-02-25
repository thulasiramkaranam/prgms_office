
from bs4 import BeautifulSoup
import requests
import json
import psycopg2
import pandas as pd
import unidecode
import sys

def scrap_new_location(search):

    try:
        search_term = ("** latitude longitude".replace("**",search))
     
        escaped_search_term = search_term.replace(' ', '+')
        #escaped_search_term = search_term
      
        google_url = 'https://www.google.com/search?q={}&num={}&hl={}'.format(escaped_search_term,1, "en")
        response = requests.get(google_url,headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text,"lxml")
        data = soup.find("div",{"class":"Z0LcW"})
        
        if data is not None:
            values = data.text
            lat = values.split(',')[0]
            lng = values.split(',')[1]
            dictt = {}
                
            dictt.update({"lat":lat, "long": lng})
            return dictt
        else:
            return None
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    
def get_pgsql_connection():
    try:
        db_name = 'senseai'
        db_user = 'neo_app_vso_root'
        db_host = 'neo-app-vso.c8yss8lfzn7r.us-east-1.rds.amazonaws.com'
        db_pass = 'Bcone,12345'
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, db_user, db_host, db_pass))
        return conn
    except Exception as e:
        print("Failed in get_pgsql_connection function")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    

def get_lat_long(input):
    try:
        city_name = input['city_name']
        sub_country = input['sub_country']
        country = input['country']
        conn_select = get_pgsql_connection()
        cursor_select = conn_select.cursor()
        if city_name != 'none':
            print("inside first select query")
            query = "select * from senseai_lat_long where city_name = %s and sub_country=%s and country=%s"
            cursor_select.execute(query, (city_name.lower(),sub_country.lower(),country.lower()))
        elif city_name == 'none' and (sub_country != 'none' or country != 'none'):
            print("second select query")
            query = "select * from senseai_lat_long where city_name = %s and sub_country=%s and country=%s"
            cursor_select.execute(query, (city_name.lower(),sub_country.lower(),country.lower()))
        else:
            return "if city is given subcountry and country are mandatory, only subcountry and country can also be given"
        result = cursor_select.fetchone()
        if result is None:
            if city_name != 'none' and sub_country != 'none' and country != 'none':
                search_string = city_name+" "+sub_country+ " "+country
            elif (sub_country != 'none' or country != 'none') and city_name == 'none':
                if sub_country == 'none':
                    search_string = country  
                else:
                    search_string = sub_country
            lat_long = scrap_new_location(search_string)
            if lat_long is None:
                return "Latitude and Longitude not found"
            else:
                insert_lat_long(city_name, sub_country, country, lat_long)
                return lat_long
            
        else:
            return result[3]
    except Exception as e:
        print("Failed in get_lat_long function")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    
 
        

def insert_lat_long(city_name, sub_country, country, lat_long):
    try:
        query = "INSERT INTO senseai_lat_long VALUES(%s,%s,%s,%s) ON CONFLICT (city_name, sub_country, country) DO NOTHING"
        conn_insert = get_pgsql_connection()
        cursor_insert = conn_insert.cursor()
        city_name = unidecode.unidecode(city_name.strip().lower())
        country = unidecode.unidecode(country.strip().lower())
        lat_long = json.dumps(lat_long)
        if sub_country is None:
            sub_country = 'none'
        else:
            sub_country = unidecode.unidecode(sub_country.strip().lower())

        
        cursor_insert.execute(query, (city_name,sub_country,country,lat_long))        
        conn_insert.commit()
        cursor_insert.close()
        print("inserted the record")
    except Exception as e:
        print("In the exception of insert lat long")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


#input = {'city_name': 'arlington','sub_country': 'washington', 'country': 'united states'}
input = {'city_name': 'Beijing','sub_country': 'beijing', 'country': 'china'}

#input2 = {'city_name': 'goa', 'country': 'india'}
result = get_lat_long(input)
print(result)
print("After result")


