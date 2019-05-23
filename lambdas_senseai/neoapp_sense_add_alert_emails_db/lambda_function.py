import json
import psycopg2
import logging
import os
logger = logging.getLogger()
logger.setLevel(logging.INFO)


conn = psycopg2.connect(host=os.environ['hostname'] ,port=5432,dbname=os.environ['dbname'],user=os.environ['dbusername'],password=os.environ['dbpassword'])
cur = conn.cursor()
logger.info("connected new code")
def lambda_handler(event, context):
    # TODO implement
    logger.info(event)
    logger.info("after event")
    logger.info(type(event))

    
    insrt_stmt = "INSERT INTO senseai_alert_log VALUES(%s,%s,%s,%s,%s,%s)"
    mailed_date = event['mailed_date']
    mailid = event['email_id']
    severity = event['severity']
    headline = event['headline']
    summary = event['summary']
    boln = True
    cur.execute(insrt_stmt, (mailed_date,mailid,severity,headline, summary, boln))
    conn.commit()
    
    print("inserted data")
   
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
