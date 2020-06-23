import boto3
from dependencies import spark_logging
from io import StringIO, BytesIO
import csv
from datetime import datetime
import pytz


class S3Utils(object):
    def __init__(self, spark, config_dict):
        self.logger = spark_logging.Log4j(spark)
        self.config_dict = config_dict
        self.s3 = boto3.resource('s3',
                                 endpoint_url=self.config_dict['s3_endpoint_url'],
                                 aws_access_key_id=self.config_dict['s3_aws_access_key_id'],
                                 aws_secret_access_key=self.config_dict['s3_aws_secret_access_key']
                                 )

    def get_files_received_after(self, bucket_name, partition_key_prefix, last_fetched_timestamp):
        bucket = self.s3.Bucket(bucket_name)
        object_path = bucket.objects.filter(Prefix=partition_key_prefix)
        files_list = []
        for obj in object_path:
            if obj.last_modified.strftime(self.config_dict['timestamp_with_tz_format']) > \
                    last_fetched_timestamp.strftime(self.config_dict['timestamp_with_tz_format']):
                self.logger.debug("List of files received after " +
                                  str(last_fetched_timestamp) + ": " + obj.key + str(obj.last_modified))
                files_list.append("s3a://" + bucket_name + "/" + str(obj.key))

        return files_list

    def list_objects_v2(self, bucket_name, prefix, next_continuation_token=""):
        max_keys = self.config_dict['s3_list_max_keys']

        if next_continuation_token == "":
            list_response = self.s3.list_objects_v2(Bucket=bucket_name,
                                                    Prefix=prefix,
                                                    MaxKeys=max_keys,
                                                    )
        else:
            list_response = self.s3.list_objects_v2(Bucket=bucket_name,
                                                    Prefix=prefix,
                                                    MaxKeys=max_keys,
                                                    ContinuationToken=next_continuation_token
                                                    )
        return list_response

    def write_object_as_csv(self, obj, bucket_name, key):
        ob_dict = vars(obj)

        csv_columns = ob_dict.keys()

        str_buff_csv = StringIO()
        writer = csv.DictWriter(str_buff_csv, dialect='excel', fieldnames=csv_columns)
        writer.writeheader()
        writer.writerow(ob_dict)

        byte_buff_csv = BytesIO(str_buff_csv.getvalue().encode())

        self.s3.Bucket(bucket_name).put_object(Key=key, Body=byte_buff_csv)
        return None

    def get_latest_key_as_csv(self, bucket_name, key_prefix):
        """
        get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

        objs = self.s3.list_objects_v2(Bucket='my_bucket')['Contents']
        last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][0]
        obj_dict = {}
        """
        bucket = self.s3.Bucket(bucket_name)
        object_list = list(bucket.objects.filter(Prefix=key_prefix))
        csv_object = ''
        latest_key = ''
        latest_key_time = datetime(1, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        self.logger.debug("Starting to list all the files at " + bucket_name + "/" + key_prefix)

        if len(object_list) > 1:
            for obj in object_list:
                if obj.last_modified > latest_key_time:
                    latest_key_time = obj.last_modified
                    latest_key = obj.key
            self.logger.debug("Latest audit file: " + latest_key)
            csv_object = self.s3.Bucket(bucket_name).Object(latest_key).get()
        return csv_object

    def is_key_prefix_empty(self, bucket_name, key_prefix):
        bucket = self.s3.Bucket(bucket_name)
        object_list = list(bucket.objects.filter(Prefix=key_prefix))
        if len(object_list) > 1:
            return True
        else:
            return False


"""
Test Section
"""

"""
my_path = path.abspath(path.dirname(__file__))
path = path.join(my_path, "../configs/global_config.json")

with open(path, 'r') as config_file:
    config_dict = json.load(config_file)
    get_s3_client(config_dict['s3'])
"""

