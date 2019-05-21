import json
import psycopg2
import os
import logging
from datetime import datetime, timedelta
logger = logging.getLogger()
logger.setLevel(logging.INFO)


conn = psycopg2.connect(host=os.environ['pgsql_host'] ,port=5432,dbname=os.environ['dbname'],user=os.environ['pgsql_username'],password =os.environ['pgsql_password'])
cur = conn.cursor()
logger.info("connected new code")
def lambda_handler(event, context):
    # TODO implement
    #mail_id = event['email']
    
    
    insert_query = "INSERT INTO senseai_chat_read_log VALUES(%s,%s,%s) on conflict (event_id, email) do update set uuid = %s"
    #insert_query = insert_query.format(event['msg_id'])
    print(insert_query)
    event_id = event['event_id']
    email_id = event['email_id']
    msg_id = event['msg_id']
    cur.execute(insert_query, (event_id,email_id,msg_id, (msg_id,))) 
    conn.commit()
    return {"data": "msg marked as read"}
    
    
