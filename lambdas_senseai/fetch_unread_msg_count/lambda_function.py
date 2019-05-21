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
    
    mail_id = event['email_id']
    #select_query_chat = "select * from senseai_chat_app where channel_name = ANY(%s);" + strng
    channel_name = event['event_ids']
    cur.execute("select * from senseai_chat_app where channel_name = ANY(%s);", (channel_name,))
    result_msgs  = cur.fetchall()
    select_query_msg_status = "select * from senseai_chat_read_log where email = %s and event_id = ANY(%s);"
    cur.execute(select_query_msg_status,(mail_id, channel_name))
    result_status = cur.fetchall()
    print(result_msgs)
    msgs_read = {}
    for i in result_status:
        msgs_read.update({i[0]: i[2]})
    
    output = {}
    for i in result_msgs:
        if i[0] not in msgs_read:
            output.update({str(i[0]): len(i[1])})
        else:
            last_msg_uid = msgs_read[i[0]]
            total_msgs = [ii['uuid'] for ii in i[1]]
            print(total_msgs)
            print(last_msg_uid)
            unread_count = len(total_msgs) - total_msgs.index(last_msg_uid) - 1
            output.update({str(i[0]): unread_count})
            
    return output
        
    
    
    
