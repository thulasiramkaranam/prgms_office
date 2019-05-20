



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
    
    
    return "success"

