import pandas as pd
import boto3

def lambda_handler(event, context):
    # TODO implement
    #mail_id = event['email']
    df = pd.DataFrame({'num_legs': [2, 4, 8, 0],
                   'num_wings': [2, 0, 0, 0],
                   'num_specimen_seen': [10, 2, 1, 8]},
                index=['falcon', 'dog', 'spider', 'fish'])
    df.to_excel("/tmp/testing.xlsx")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('/tmp/testing.xlsx', 'neo-apps-procoure.ai', 'output/upload_testing.xlsx')