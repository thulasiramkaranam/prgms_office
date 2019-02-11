
import boto3
import time
import json
import paramiko
import ssh
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    ec2_client = boto3.client('ec2')
    response = ec2_client.start_instances(
        InstanceIds=[
        'i-0bbe7527407062453'
        ])
    # i-0bbe7527407062453 is the ec2 instance id
    time.sleep(60) # In order to make sure that the instance is running before giving commands
    logger.info(event)
    logger.info("After event")
    s3_client = boto3.client('s3')
    s3_client.download_file('thulasi-ram','ec2andemr.pem', '/tmp/ec2.pem')
    # Downloading the pem file from s3 bucket thulasiram-ram to lambda temp folder
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    privkey = paramiko.RSAKey.from_private_key_file('/tmp/ec2.pem')
    #instance_id = "i-058481edd4788c01c"
    ssh.connect('ec2-18-221-144-29.us-east-2.compute.amazonaws.com',username='ec2-user',pkey=privkey)
    # ssh to the ec2 instance using pem file downloaded into lambda temp folder.
    stdin, stdout, stderr = ssh.exec_command('echo "ssh to ec2 instance successful"') # Excute the commands here
    
    data = stdout.read().splitlines()
    for line in data:
        logger.info(line)
        logger.info("commands output")
    return "commands executed"


