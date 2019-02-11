import json
import boto3
import sys
import os
import logging
import datetime
from datetime import datetime as dtt
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    
    
    #s3_file_url = os.environ['S3_Bucket_URL']+event['Records'][0]['s3']['object']['key']
    #logger.info(s3_file_url)
    logger.info("After logging")
    instance_id = "j-1L8WALGDUHKBF"
    client = boto3.client('dynamodb')
    table = boto3.resource('dynamodb').Table('NeoApp_Emr_status_monitor')
    response = table.get_item(Key={'Emr_id': 'J-12345'})
    logger.info(response)
    logger.info("After response")
    exp_attribute_values = {}
    update_exp = ""
    table_key={}
    table_key.update({'Emr_id':"k-123"})
    status = {"Emr_status": "1"}
    for key, value in status.items():
        update_exp +=  key + ' = :' + key + ','
        key_placeholder  = ':' + str(key)
        exp_attribute_values[key_placeholder] = status[key]
   
    update_exp = "SET " + update_exp.rstrip(",")
    response = table.update_item(Key=table_key,
                        UpdateExpression=update_exp,                   
                        ExpressionAttributeValues=exp_attribute_values,
                        ReturnValues="UPDATED_NEW")
    
    try:
        print ("sucss")
        # connection = boto3.client('emr')
        # cluster_id = connection.run_job_flow(
        # Name='test_emr_boto3',
        
        # ReleaseLabel='emr-4.2.0',
        # Instances={
        #     'InstanceGroups': [
        #         {
        #             'Name': "Master nodes",
        #             'Market': 'ON_DEMAND',
        #             'InstanceRole': 'MASTER',
        #             'InstanceType': 'm1.large',
        #             'InstanceCount': 1,
        #         },
        #         {
        #             'Name': "Slave nodes",
        #             'Market': 'ON_DEMAND',
        #             'InstanceRole': 'CORE',
        #             'InstanceType': 'm1.large',
        #             'InstanceCount': 2,
        #         }
        #     ],
        #     'Ec2KeyName': 'neo-app-emr',
        #     'KeepJobFlowAliveWhenNoSteps': True,
        #     'TerminationProtected': False,
        #     'Ec2SubnetId': 'subnet-18d10d36',
        # },
        # Steps=[],
        # VisibleToAllUsers=True,
        # JobFlowRole='neo-app_EMR_EC2_Default_Role',
        # ServiceRole='neo-app_EMR_Default_Role',
        # Tags=[
        #     {
        #         'Key': 'test',
        #         'Value': 'boto3'
        #     }
        # ],
        #     )
        # return {
        #     "statusCode": 200,
        #     "body": json.dumps('Hello from Lambda!')
        # }
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),type(e).__name__, e)
