import urllib.request, urllib.parse, urllib.error
import boto3
import ast
import json
import os
import time
import re
from botocore.exceptions import ClientError
print('Loading function')
def lambda_handler(event, context):
    client_name = os.environ['CLIENT_NAME']
    exclude_list = ast.literal_eval(os.environ['EXCLUDE_LIST']) # The list include which broker does not allow copy
    enable_copy = os.environ['ENABLE_COPY']

    s3 = boto3.client('s3')
    sns_message = ast.literal_eval(event['Records'][0]['Sns']['Message'])
    target_bucket = "${target_bucket}"
    source_bucket = str(sns_message['Records'][0]['s3']['bucket']['name'])
    key = str(urllib.parse.unquote_plus(sns_message['Records'][0]['s3']['object']['key']).decode('utf8')) # The source file path
    copy_source = {'Bucket':source_bucket, 'Key':key}
    date = time.strftime("%Y%m%d",time.localtime(time.time()))
    separator = '/'
    source_file_path = key[:key.rindex(separator)]
    source_file_name = key[key.rindex(separator)+1:]

    file_parent_index = -1 # This variable represents the location of prod/uat folder
    path_array = key.split('/')
    lower_path_array =  [item.lower() for item in path_array]
    if str.lower(client_name) == 'kpmg':
        p = re.compile('prod|uat')
        temp_list = [item for item in lower_path_array if p.findall(item)]
        if (len(temp_list) > 0) and (temp_list[0] in lower_path_array):
            file_parent_index = lower_path_array.index(temp_list[0])
    else:
        if 'prod' in  lower_path_array:
            file_parent_index = lower_path_array.index('prod')
        elif 'uat' in lower_path_array:
            file_parent_index = lower_path_array.index('uat')

    # If client is kpmg, which means no prod/uat folder, the archived folder will be created under the broker root directory
    if str.lower(client_name) == 'kpmg':
        source_archive_file_path = source_file_path
    else:
        source_archive_file_path = source_file_path[:source_file_path.rindex(separator)]

    # Loop this exclude list, if the file path in this exclude list, don't perform copy and archive operation
    not_exclude_folder = True
    for item in exclude_list:
        if source_file_path.startswith(item):
            not_exclude_folder = False
            break
    if not_exclude_folder:
        source_archive_file = source_archive_file_path + separator + date + separator + source_file_name
        try:
            # If the file is posted to /prod or /uat, then archive it.
            # Avoid copying the archived file again when archived file is put to s3
            if (file_parent_index != -1) and (source_archive_file != key) and (source_file_path.find(date) == -1):
                s3.copy_object(CopySource = copy_source, Bucket = source_bucket, Key = source_archive_file)
                print('File: %s archived successfully.' % key)
        except ClientError as e:
            print(e)
    else:
        print('File: %s is excluded.' % key)

    # If the file path is not in exclude list, post file to prod/uat folder and the account enable copy then copy it to target folder.
    if not_exclude_folder and (enable_copy == 'true') and (file_parent_index != -1) and (source_file_path.find(date) == -1):
        sts_client = boto3.client('sts') # Target client
        assumedRoleObject = sts_client.assume_role(
            RoleArn="${destination_role}",           
            RoleSessionName="AssumeRoleSession1"
        )
        credentials = assumedRoleObject['Credentials']
        s3 = boto3.client(
            's3',
            aws_access_key_id = credentials['AccessKeyId'],
            aws_secret_access_key = credentials['SecretAccessKey'],
            aws_session_token = credentials['SessionToken'],
        )

        target_folder = 'UAT'
        source_folder = path_array[file_parent_index]
        # The target folder should have the same format as the source folder
        if source_folder.islower():
            target_folder = target_folder.lower()
        elif source_folder.istitle():
            target_folder = target_folder.title()

        if str.lower(client_name) == 'kpmg':
            target_file = source_archive_file_path[:source_archive_file_path.rindex('-') + 1] + target_folder + separator + source_file_name
        else:
            target_file = source_archive_file_path + separator + target_folder + separator + source_file_name
        try:
            s3.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=target_file)
            print('Copying %s from bucket %s to bucket %s, target file:%s.' % (key, source_bucket, target_bucket, target_file))
        except ClientError as e:
            print(e)