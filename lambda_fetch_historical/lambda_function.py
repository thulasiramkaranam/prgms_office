

import json
import requests
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    c = 'http://172.16.36.156:80/fetch_historical'
    dd = {"channel_name": "GP100259"}
    r = requests.post(url = c, data = json.dumps(dd))
    logger.info(r)
    return r
cd = lambda_handler(1,2)
print(cd)