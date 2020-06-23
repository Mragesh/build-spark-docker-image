"""
InstrumentLogFileUtils.py
~~~~~~~~

Module to get partitions for last x days 
"""

from datetime import date, timedelta
from dependencies import spark_logging
from dependencies.S3Utils import S3Utils
import csv
import pytz
from io import StringIO
from datetime import datetime


class InstrumentLogFileUtils(object):

    def __init__(self, spark, config_dict):
        self.logger = spark_logging.Log4j(spark)
        self.config_dict = config_dict
        self.spark = spark

    def get_valid_partitions_keys(self):
        """ Get Valid Partitions

        Method to get list of partitions for last x days
        """
        date_list = [(date.today() - timedelta(days=i)).strftime("%Y-%m-%d")
                     for i in range(int(self.config_dict['instrument_log_read_period']))]
        partition_key_prefix = self.config_dict['instrument_log_key'] + self.config_dict['partition_prefix']
        partition_key_list = list(map(lambda x: partition_key_prefix + x, date_list))
        self.logger.debug("List of partitions for last " + self.config_dict['instrument_log_read_period'] + " days: " +
                          str(partition_key_list))
        return partition_key_list

    def get_incremental_files(self, partition_keys_prefix):
        """ Get Incremental Files

            Method to get the files received since the last successful run
        """

        s3_utils = S3Utils(self.spark, self.config_dict)
        csv_object = s3_utils.get_latest_key_as_csv(self.config_dict['s3_etl_runs_bucket'],
                                                    self.config_dict['s3_etl_runs_audit_key'])
        last_fetched_timestamp = datetime(1, 1, 1, 0, 0, 0, tzinfo=pytz.utc)

        if csv_object:
            self.logger.debug("Latest audit details: " + str(csv_object['Body']))

            csv_content = csv_object['Body'].read()

            csv_string = StringIO(csv_content.decode('UTF-8'))
            for row in csv.DictReader(csv_string):
                last_fetched_timestamp = datetime.strptime(row['etl_start_time'],
                                                           self.config_dict['timestamp_with_tz_format'])

        incremental_file_list = s3_utils.get_files_received_after(self.config_dict['instrument_log_bucket'],
                                                                  partition_keys_prefix,
                                                                  last_fetched_timestamp)

        return incremental_file_list

    def get_files_to_be_processed(self):
        """ Get instrument logs to be processed
        Get list of files that were received since last run

        :return: list of files to be processed by ETL jobs
        """
        # Path for files list
        files_list = []

        # Get a list of valid partitions for the look-up
        valid_partitions_keys = self.get_valid_partitions_keys()

        for key_prefix in valid_partitions_keys:
            files_list.extend(self.get_incremental_files(key_prefix))
            self.logger.debug(files_list)
        return files_list


"""
Test section
"""
"""
my_path = path.abspath(path.dirname(__file__))
path = path.join(my_path, "../configs/global_config.json")

with open(path, 'r') as config_file:
    config_dict = json.load(config_file)
    get_files_to_be_processed(config_dict['s3'])
"""
