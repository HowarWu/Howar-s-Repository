import urllib.request, urllib.parse, urllib.error
import boto3
import ast
import os
import datetime
import re
from botocore.exceptions import ClientError

print('Loading function')
  
def lambda_handler(event, context):
    client_name = os.environ['CLIENT_NAME']
    s3 = boto3.client('s3')
    folder_list = ast.literal_eval(event.get('Folders'))
    source_bucket = "${source_bucket}"
    for item in folder_list:
        try:
            response = s3.list_objects(Bucket=source_bucket,Prefix=item)
            files = response['Contents']
            file_parent_index = -1 # This variable represents the location of prod/uat folder
            for file in files:
                file_full_path = str(urllib.parse.unquote_plus(file.get('Key')).decode('utf8'))
                path_array = file_full_path.split('/')
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
                if file_parent_index != -1:
                    # If find the prod/uat folder, jump out of the loop 
                    break
            for file in files:
                file_full_path = str(urllib.parse.unquote_plus(file.get('Key')).decode('utf8'))
                path_array = file_full_path.split('/')
                archived_date = ''
                try:
                    if (str.lower(client_name) == 'kpmg') and (file_parent_index != -1) and (len(path_array) > (file_parent_index + 1)) and (path_array[file_parent_index + 1] != ''):
                        archived_date = datetime.datetime.strptime(path_array[file_parent_index + 1], '%Y%m%d')
                    elif (str.lower(client_name) != 'kpmg') and (file_parent_index != -1):
                        archived_date = datetime.datetime.strptime(path_array[file_parent_index], '%Y%m%d')
                    current_date = datetime.datetime.now()
                    if (archived_date != '') and ((current_date - archived_date).days > 30):
                        s3.delete_object(Bucket=source_bucket, Key=file_full_path)
                        print('Archived folder: %s deleted successfully.' % file_full_path)
                except ValueError as e:
                    # If we can't convert the file path to date that means it isn't the archived folder, just skip it
                    print('File: %s is not archived file, skip it.' % file_full_path)
        except Exception as e:
            print(e)
