
import json
import psycopg2
def make_conn():
    db_name = 'senseai'
    db_user = 'neo_app_vso_root'
    db_host = 'neo-app-vso.c8yss8lfzn7r.us-east-1.rds.amazonaws.com'
    db_pass = 'Bcone,12345'
    conn = None
    result = []
    query = "select * from senseai_lat_long"
    try:
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, db_user, db_host, db_pass))
        # cursor = conn.cursor()
        # cursor.execute(query)
        # raw = cursor.fetchall()
        # for line in raw:
        #     print(line)
        #     print("After line")
        #     result.append(line)
    
    
    except:
        print("I am unable to connect to the database")
    return conn


def lambda_handler(event, context):
    # TODO implement
    
    conn_obj = make_conn()
    print(type(conn_obj))
    print("After connectioin object")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
lambda_handler(1,2)


