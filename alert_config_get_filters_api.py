
import json
import boto3
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    
    table = boto3.client('dynamodb', region_name='us-east-1')
    response = table.scan(
            TableName='Neoapp_sense_crawler',
            
            AttributesToGet=[
                'Website', 'project_code'
                ]
                    )
    logger.info(response)
    logger.info("after response")
    output = {}
    event_attributes = ['All', 'Tags', 'Summary', 'Headline']
    sources = []
    for record in response['Items']:
        sources.append(record['Website']['S'])
        if 'project_code' in record:
            sources.append(record['project_code']['S'])
    sources.extend(["All", "Default"])
    output.update({"sources": sources, "event_attributes": event_attributes})
     
    return output
