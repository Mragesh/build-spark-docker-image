import __main__
import boto3
import json
from os import path

def get_s3_client(config_s3_):
    s3 = boto3.resource('s3',
                    endpoint_url=config_s3_['endpoint_url'],
                    aws_access_key_id=config_s3_['aws_access_key_id'],
                    aws_secret_access_key=config_s3_['aws_secret_access_key']
    )

    
"""
Test Section
"""
my_path = path.abspath(path.dirname(__file__))
path = path.join(my_path, "../configs/global_config.json")

with open(path, 'r') as config_file:
    config_dict = json.load(config_file)
    get_s3_client(config_dict['s3'])
