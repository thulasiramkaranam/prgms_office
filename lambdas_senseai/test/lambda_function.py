import json
import requests
import boto3
def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')

    resp = s3.generate_presigned_url('get_object',Params={'Bucket':
        'us-east-1-neo-app-senseai',
        'Key': 'sense_refresh_bc/2019-05-23refresh.xlsx'},

        ExpiresIn=345)
    file = requests.get(resp)
    print(file.code)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
