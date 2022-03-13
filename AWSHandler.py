import boto3
import requests
import json
import yaml
import pandas as pd
import os.path
import numpy as np


class AWSHandler:

    def __init__(self):
        with open("scistarter_cfg.yml", "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        self.__S3_CONFIG = cfg['aws_s3']
        self.__client = boto3.client("s3", **self.__S3_CONFIG)

    def get_buckets_list(self):
        buckets_names = []
        try:
            response = self.__client.list_buckets()
            for bucket in response['Buckets']:
                buckets_names.append(bucket['Name'])
        except Exception as e:
            raise Exception(f'Failed to fetch buckets name, error: {e}')
        return buckets_names

    def get_log_keys(self, bucket_name):
        objects = self.__get_bucket_objects(bucket_name)
        keys = {}
        for entry in objects['Contents']:
            keys[entry['Key']] = entry['LastModified']
        sorted(keys, key=keys.get)
        return keys

    def __get_bucket_objects(self, bucket_name):
        objects = []
        try:
            objects = self.__client.list_objects(Bucket=bucket_name)
        except Exception as e:
            raise Exception(f'Failed to fetch buckets name, error: {e}')
        return objects


class IPHandler:

    def __init__(self):
        self.request_url = 'https://geolocation-db.com/jsonp/'

    def lookup_ip(self, ip_addr):
        try:
            response = requests.get(f'{self.request_url}{ip_addr}')
            result = response.content.decode()
            # Clean the returned string so it just contains the dictionary data for the IP address
            result = result.split("(")[1].strip(")")
            # Convert this data into a dictionary
            result  = json.loads(result)
            return result
        except Exception as e:
            raise Exception(f'Failed to send or read IP API request, error: {e}')


ip_handler = IPHandler()
ip_handler.lookup_ip('132.72.67.237')