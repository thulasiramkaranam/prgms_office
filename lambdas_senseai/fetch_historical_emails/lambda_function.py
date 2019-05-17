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
    if event['limit'] == 5:
        fromtime = str(datetime.now() - timedelta(days=5))[:10]
        totime = str(datetime.now())[:10]
    elif event['limit'] == 'all':
        fromtime = str(datetime.now() - timedelta(days=7))[:10]
        totime = str(datetime.now())[:10]
    else:
        return "Limit is mandatory"
        
        
    
    query = "select * from alert_log where name = %s and event_date >= %s and event_date <= %s"
    cur.execute(query, (event['mail'], fromtime, totime))
    table_records = cursor.fetchall()
    logger.info(table_records)
    logger.info(type(table_records))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
