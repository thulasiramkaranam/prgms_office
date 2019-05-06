




from flask import Flask, render_template, request
from pusher import Pusher
from flask_cors import CORS
import json
import psycopg2
import uuid
import sys
import datetime
from datetime import datetime as dtt
app = Flask(__name__)
CORS(app)
# configure pusher object
pusher = Pusher(
app_id='740844',
key='9c79cb87012c5c3a5ce2',
secret='7a5ebd5a551edd1fd0a4',
cluster='ap2',
ssl=True)
conn = None
def fetch_postgres_db_session():

    try:
        global conn
        if conn is None:
            db_name = 'neo-app-sense'
            db_user = 'neo_app_sense_root'
            db_host = 'neo-app-sense.c5appvbypuuj.us-east-1.rds.amazonaws.com'
            db_pass = 'nkaFwT6v'
            conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, db_user, db_host, db_pass))
            print("returned the conn")
        return conn
    except Exception as e:
        print("Failed in get_pgsql_connection function")
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/orders', methods=['POST'])
def order():
    
    return "units logged"

def insert_update_chat_db(chat_data,insrt_update):
    print(insrt_update)
    print(chat_data)
    dictt = {}
    dictt.update({"message": chat_data['message'], 'type': 'human', 
            'Channelname': chat_data['Channelname'],
                'Eventname': chat_data['Channelname']+'event',
                    'displayName':chat_data['displayName'],
                    'email': chat_data['email'],
                    'createdAt': str(dtt.now()) })
    if insrt_update == 'insert':

        insrt_query = """ INSERT INTO senseai_chat_app (channel_name, historical_chat) VALUES (%s,%s)"""
        pg_obj_insrt = fetch_postgres_db_session()
        cursor_insrt = pg_obj_insrt.cursor()
        insrt_data = [dictt]
        insrt_data = json.dumps(insrt_data)
        print(insrt_data)
        print("after insrt data")
        chnl_name = chat_data['Channelname']
        cursor_insrt.execute(insrt_query, (chnl_name,insrt_data))
        pg_obj_insrt.commit()
        cursor_insrt.close()
    if insrt_update == 'update':
        pg_obj_update = fetch_postgres_db_session()
        cursor_update = pg_obj_update.cursor()
        update_query = """UPDATE senseai_chat_app SET historical_chat = historical_chat::jsonb || (%s)::jsonb where channel_name = (%s)"""
        dictt = {}
        dictt.update({"message": chat_data['message'], 'type': 'human', 
            'Channelname': chat_data['Channelname'],
                'Eventname': chat_data['Channelname']+'event',
                    'displayName':chat_data['displayName'],
                    'email': chat_data['email'],
                    'createdAt': str(dtt.now()) })
        update_data = json.dumps(dictt)
        chnl_name = chat_data['Channelname']
        cursor_update.execute(update_query, (update_data,chnl_name))
        pg_obj_update.commit()
        cursor_update.close()

    


def check_channel(chanl_name):
    query = "select * from senseai_chat_app where channel_name = %s"
    pg_obj = fetch_postgres_db_session()
    cursor_select = pg_obj.cursor()
    cursor_select.execute(query, (chanl_name,))
    result = cursor_select.fetchone()
    if result is None:
        return "No Historical chat available"
    else:
        return result[1]

@app.route('/fetch_historical', methods = ['POST'])
def fetch_historical():
    pg_obj = fetch_postgres_db_session()
    data = request.data
    data = json.loads(data)
    cursor_select = pg_obj.cursor()
    query = "select * from senseai_chat_app where channel_name = %s"
    channel_name = data['channel_name']
    print(channel_name)
    print("after channel name")
    cursor_select.execute(query, (channel_name,))
    result = cursor_select.fetchone()
    if result is None:
        return json.dumps([{"data": "No Historical chat available"}])
    else:
        return json.dumps(result[1])

@app.route('/message', methods=['POST'])
def message():
    
    data = request.data
    print(data)
    print("After data message")
    data = json.loads(data)
    eject_data = {"message": data["message"], "email": data["email"],"displayName":data['displayName'],"eventID":data['eventID'], "createdAt": str(dtt.now())}
    eject_data = json.dumps(eject_data)
    print("ahsaj", data)
    table_data = check_channel(data['Channelname'])
    if table_data == 'No Historical chat available':
        insert_update_chat_db(data, 'insert')
    else:
        insert_update_chat_db(data, 'update')


    pusher.trigger(data['Channelname'],data['Channelname']+'event', eject_data)
    
    return eject_data

@app.route('/customer', methods=['POST'])
def customer():
    data = request.form
    pusher.trigger(u'customer', u'add', {
        u'name': data['name'],
        u'position': data['position'],
        u'office': data['office'],
        u'age': data['age'],
        u'salary': data['salary'],
    })
    return "customer added"

@app.route('/get', methods=['POST'])
def testing():
    
    data = request.data
    data = json.loads(data)
    print(data)
    print("After data initial")
    chat = {"createdAt": str(dtt.now()),
            "type": "joined",
            "id": str(uuid.uuid4()),
            "email": data['email'],
            "eventID":data['eventID'],
            "displayName":data['displayName']
                }
    cd = json.dumps(chat)
    pusher.trigger(data['Channelname'],data['Channelname']+'event', chat )
    print(data)
    print("called get api")
    
    return cd

if __name__ == '__main__':
    app.run(host = '172.16.36.156', port = 80, debug=True)