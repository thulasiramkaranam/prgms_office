

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
    
        
        
    fromtime = event['from_time'][:10]
    totime = event['to_time'][:10]
    query = "select * from trending_topic where trend_date >= %s and trend_date <= %s"
    cur.execute(query, (fromtime, totime))
    table_records = cur.fetchall()
    logger.info(table_records)
    logger.info(type(table_records))
    data = []
    for i in table_records:
        dictt = {}
        dictt.update({"event_date": str(i[1]),
                    "event_headline": str(i[0])})
        data.append(dictt)
    return {"data": data}
    
    
