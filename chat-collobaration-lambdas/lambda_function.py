



import json
import requests
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    url_fetch = 'http://172.17.8.82:5000/fetch_historical'
    
    resp = requests.post(url = url_fetch, data = json.dumps(event))
    logger.info(resp)
    return (json.loads(resp.text))
