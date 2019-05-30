


from bs4 import BeautifulSoup
import requests
import json
import psycopg2
import pandas as pd
import unidecode
import sys

def get_pgsql_connection_neoapps():
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
    
def get_pgsql_connection_senseai():
    try:
        db_name = 'senseai'
        db_user = 'neo_app_sense_root'
        db_host = 'neo-app-sense1.c5appvbypuuj.us-east-1.rds.amazonaws.com'
        db_pass = 'nkaFwT6v'
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, db_user, db_host, db_pass))
        return conn
    except Exception as e:
        print("Failed in get_pgsql_connection function")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    



def select_data():

    conn_select = get_pgsql_connection_neoapps()
    cursor_select = conn_select.cursor()
    query = "select * from senseai_lat_long"
    cursor_select.execute(query)
    records = cursor_select.fetchall()
    conn_insert = get_pgsql_connection_senseai()
    cursor_insert = conn_insert.cursor()
    insert_query = "INSERT INTO senseai_lat_long VALUES(%s,%s,%s,%s) ON CONFLICT (city_name, sub_country,           country) DO NOTHING"
    for record in records:

        
        city_name = record[0]
        sub_country = record[1]
        country = record[2]
        lat_long = json.dumps(record[3])
        cursor_insert.execute(insert_query, (city_name,sub_country,country,lat_long)) 
    conn_insert.commit()
    cursor_insert.close()

select_data()
