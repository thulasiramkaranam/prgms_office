

import json
import paramiko
import boto3

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    logger.info(event)
    logger.info("After event")
    s3_client = boto3.client('s3')
    s3_client.download_file('thulasi-ram-dum','ec2andemr.pem', '/tmp/ec2.pem')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    privkey = paramiko.RSAKey.from_private_key_file('/tmp/ec2.pem')
    #instance_id = "i-058481edd4788c01c"
    ssh.connect('ec2-18-221-144-29.us-east-2.compute.amazonaws.com',username='ec2-user',pkey=privkey)
    stdin, stdout, stderr = ssh.exec_command('echo "ssh to ec2 instance successful"')
    data = stdout.read().splitlines()
    for line in data:
        print (line)
    return {
        "statusCode": 200,
        "body": json.dumps('Hello from Lambda!')
    }


