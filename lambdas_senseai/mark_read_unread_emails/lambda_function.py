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
    mailid = event['mailid']
    for row in event['mails']:
        row = row.strip()
        update_stmt = "update senseai_alert_log set unread = %s where email = %s and headline = %s;"    
        
        cur.execute(update_stmt, (False, mailid, row))
        
        conn.commit()
        
    return {
        "data": "updated successfully"
    }
