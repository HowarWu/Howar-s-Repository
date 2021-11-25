import urllib.request, urllib.parse, urllib.error
import boto3
import ast
import os
import datetime
import re
import json
import time
from botocore.exceptions import ClientError
 
print('Loading function')
 
def lambda_handler(event, context):
   client_name = os.environ['CLIENT_NAME']
   s3 = boto3.client('s3')
   ssm_client = boto3.client('ssm')
   folder_list = ast.literal_eval(event.get('Folders'))
   source_bucket = "${source_bucket}"
   for item in folder_list:
       try:
           response = s3.list_objects(Bucket=source_bucket,Prefix=item)
           files = response['Contents']
           for file in files:
               file_full_path = str(urllib.parse.unquote_plus(file.get('Key')).decode('utf8'))
               path_array = file_full_path.split('/')
               lower_path_array =  [item.lower() for item in path_array]
               file_parent_index = -1 # This variable represents the location of prod/uat folder
               # Use shell command to remove files in SFTP EC2 instance
               # Use '/mnt/s3' as prefix to get the real file path
               # To avoid situations where the command execution fails due to parenthesis in the file name,
               # Need to escape the real file path
               command = "rm -rf " + "\"/mnt/s3/" + file_full_path + "\""
               if str.lower(client_name) == 'kpmg':
                   p = re.compile('prod|uat')
                   temp_list = [item for item in lower_path_array if p.findall(item)]
                   if (len(temp_list) > 0) and (temp_list[0] in lower_path_array):
                       file_parent_index = lower_path_array.index(temp_list[0])
                   try:
                       if (len(path_array) > (file_parent_index + 1)) and (path_array[file_parent_index + 1] != '') and (file_parent_index != -1):
                           datetime.datetime.strptime(path_array[file_parent_index + 1], '%Y%m%d')
                   except ValueError as e:
                       # If we can't convert the file path to date that means it isn't the archived folder
                       # Send command to EC2
                       response = ssm_client.send_command(
                               InstanceIds=['${source_instance_id}'],
                               DocumentName="AWS-RunShellScript",
                               Parameters={'commands': [command]})
                       command_id = response['Command']['CommandId']
                       # In order to make sure get the command_id, add a sleep time before invoking function get_command_invocation
                       time.sleep(0.5)
                       output = ssm_client.get_command_invocation(
                               CommandId=command_id,
                               InstanceId='${source_instance_id}'
                       )
                       print('File: %s deleted successfully.' % file_full_path)
               else:
                   if 'prod' in  lower_path_array:
                       file_parent_index = lower_path_array.index('prod')
                   elif 'uat' in lower_path_array:
                       file_parent_index = lower_path_array.index('uat')
                   if (len(path_array) > (file_parent_index + 1)) and (path_array[file_parent_index + 1] != '') and (file_parent_index != -1):
                       response = ssm_client.send_command(
                               InstanceIds=['${source_instance_id}'],
                               DocumentName="AWS-RunShellScript",
                               Parameters={'commands': [command]})
                       command_id = response['Command']['CommandId']
                       time.sleep(0.5)
                       output = ssm_client.get_command_invocation(
                               CommandId=command_id,
                               InstanceId='${source_instance_id}'
                       )                        
                       print('File: %s deleted successfully.' % file_full_path)
       except Exception as e:
           print(e)