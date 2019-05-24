import json
import psycopg2
import os
import logging
import datetime
from datetime import datetime as dtt
from datetime import timedelta
logger = logging.getLogger()
logger.setLevel(logging.INFO)


conn = psycopg2.connect(host=os.environ['pgsql_host'] ,port=5432,dbname=os.environ['dbname'],user=os.environ['pgsql_username'],password =os.environ['pgsql_password'])
cur = conn.cursor()
logger.info("connected new code")
def lambda_handler(event, context):
    # TODO implement
    mail_id = event['email']
    if event['limit'] == 5:
        fromtime = str(dtt.now() - timedelta(days=5))[:10]
        totime = str(dtt.now())[:10]
    elif event['limit'] == 'all':
        fromtime = str(dtt.now() - timedelta(days=7))[:10]
        totime = str(dtt.now())[:10]
    else:
        return "Limit is mandatory"
        
    query = "select * from senseai_alert_log where email = %s and event_date >= %s and event_date <= %s"
    cur.execute(query, (mail_id, fromtime, totime))
    table_records = cur.fetchall()
    
    final_output = {}
    data = []
    for i in table_records:
        
        mailed_epoch_time = int((dtt.strptime(str(i[0]), "%Y-%m-%d")-datetime.datetime(1970,1,1)).total_seconds())
        
        dictt = {}
        dictt.update({"severity": i[2], "title": i[3], "timestamp": mailed_epoch_time, "unread": i[5]})
        data.append(dictt)
    
    final_output.update({"status": 200, "message": "Data Fetched Successfully",
          "data": data  
        })
    return final_output