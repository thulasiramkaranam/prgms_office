


import json
import boto3
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # TODO implement
    
    table = boto3.client('dynamodb', region_name=os.environ['table_region'])
    final_output = []
    response = table.scan(
            TableName=os.environ['table_name'],
            
            AttributesToGet=[
                'Website', 'project_code', 'Configuration'
                ]
                    )
    final_output.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            TableName=os.environ['table_name'],
            
            AttributesToGet=[
                'Website', 'project_code', 'Configuration'
                ]
                    )
        final_output.extend(response['Items'])
    output = []
    first_input = {"ID": "1", "name": "ALL", "expanded": True}
    projects_considered = []
    output.append(first_input)
    eo_counter = 1
    general_counter = 1
    for record in final_output:
        if 'Configuration' in record:
            if 'website_type' in record['Configuration']['M']:
                if record['Configuration']['M']['website_type']['S'] == 'EO':
                    
                    if 'EO' not in projects_considered:
                        projects_considered.append("EO")
                        output.append({"ID": "1_1", "categoryId": 1, "name": "EOC001"})
                    
                    eo_dictt = {}
                    
                    eo_dictt.update({"ID": "1_1_1"+str(eo_counter), "categoryId": "1_1", "name":record['Website']['S'] })
                    output.append(eo_dictt)
                    eo_counter += 1
                else:
                    
                    if 'generic' not in projects_considered:
                        projects_considered.append("generic")
                        output.append({"ID": "1_2", "categoryId": 1, "name": "GEN001"})
                    genric_dictt = {}
                    genric_dictt.update({"ID": "1_2_1"+str(general_counter), "categoryId": "1_2", "name":record['Website']['S'] })
                    output.append(genric_dictt)
                    general_counter += 1
        else:
            if 'project_code' in record:
                if record['project_code']['S'] == 'GEN001':
                    if 'generic' not in projects_considered:
                        projects_considered.append("generic")
                        output.append({"ID": "1_2", "categoryId": 1, "name": "GEN001"})
                    genric_dictt = {}
                    genric_dictt.update({"ID": "1_2_1"+str(general_counter), "categoryId": "1_2", "name":record['Website']['S'] })
                    output.append(genric_dictt)
                    general_counter += 1
                if record['project_code']['S'] == 'EOC001':
                    if 'EO' not in projects_considered:
                        
                        projects_considered.append("EO")
                        output.append({"ID": "1_1", "categoryId": 1, "name": "EOC001"})
                    
                    eo_dictt = {}
                    
                    eo_dictt.update({"ID": "1_1_1"+str(eo_counter), "categoryId": "1_1", "name":record['Website']['S'] })
                    output.append(eo_dictt)
                    eo_counter += 1       
   
    return output
