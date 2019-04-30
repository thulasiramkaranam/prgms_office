

import boto3
s3 = boto3.client('s3')
# Run the below line of code after uploading file
resp = s3.generate_presigned_url('get_object',Params={'Bucket': 'neo-apps-procoure.ai',

                                                            'Key': 'Raw_Files/linux_commands.txt'},
                                                    ExpiresIn=345)

# resp is the url we need to mail
# Raw_Files is the folder where file is placed
# 345 is the number of seconds user can access the file