

import json
import requests
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    
    logger.info(event)
    logger.info("after event")
    
    if 'differentiator' in event and event['differentiator'] == 'message':
        url_msg = 'http://172.16.36.156:80/message'
        r = requests.post(url = url_msg, data = json.dumps(event))
    else:
        url_fetch = 'http://172.16.36.156:80/get'
        r = requests.post(url = url_fetch, data = json.dumps(event))
    
    return json.loads(r.text)
