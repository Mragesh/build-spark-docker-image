"""
get_partitions.py
~~~~~~~~

Module to get partitions for last x days 
"""
import logging;
from datetime import date, timedelta


class LogFiles(object):

    def __init__(self, spark, config_dict):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)
        self.logger.debug("Obtained Logger")

        self.config_dict = config_dict
        self.get_valid_partitions_keys()

    def get_valid_partitions_keys(self):
        """ Get Valid Partitions

        Method to get list of partitions for last x days
        """
        date_list = [(date.today() - timedelta(days=i)).strftime("%Y-%m-%d")
                     for i in range(int(self.config_dict['instrument_log_read_period']))]
        partition_key_prefix = self.config_dict['instrument_log_key']+self.config_dict['partition_prefix']
        partition_key_list = list(map(lambda x: partition_key_prefix + x, date_list))
        self.logger.debug("List of partitions for last " + self.config_dict['instrument_log_read_period'] + " days: ")
        return partition_key_list

    def delete_old_partitions(self):
        """ Delete Old Partitions

        Method to delete partitions older than x days
        TO-DO
        """
        return None

    def get_incremental_files(self):
        """ Get Incremental Files

            Method to get the files received since the last successful run
        """


    def get_files_to_be_processed(self, config_s3_):
        """ Get instrument logs to be processed
        Get list of files that were received since last run
        """
        # Get a list of valid partitions for the look-up
        valid_partitions = self.get_valid_partitions_keys(config_s3_)
        files_list = valid_partitions
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
