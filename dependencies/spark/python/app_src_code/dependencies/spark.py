"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from dependencies import spark_logging
from dependencies.S3Utils import S3Utils
from dependencies.AuditModel import Audit
from datetime import datetime
import pytz


def start_spark(app_name='my_spark_app'):
    """Start Spark session, get Spark logger and load config files.

    This function also looks for files ending in 'config.json' that
    is sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the program exits.

    :param app_name: Name of Spark app.
    :return: A tuple of references to the Spark session, logger and
        config dict.
    """
    # create session and retrieve Spark logger object
    spark_sess = SparkSession.builder.getOrCreate()
    spark_logger = spark_logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()

    config_files = [filename for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        # path_to_config_file = path.join(spark_files_dir, config_files[0])
        configs_path_list = map(lambda x: spark_files_dir + "/" + x, config_files)
        config_dict = {}
        for config_file_path in configs_path_list:
            with open(config_file_path, 'r') as config_file:
                config_dict.update(json.load(config_file))
                spark_logger.warn('loaded config from ' + config_file_path)
    else:
        spark_logger.error('No Config File Found, Exiting')
        config_dict = None
        exit(1)

    # Set log level for rest of the session from config
    if config_dict['spark_log_level']:
        spark_sess.sparkContext.setLogLevel(config_dict['spark_log_level'])

    # set audit details
    audit = Audit(spark_sess.sparkContext.getConf().get('spark.app.id'), config_dict['etl_job_name'],
                  datetime.now(pytz.utc).strftime(config_dict['timestamp_with_tz_format']))

    return spark_sess, spark_logger, config_dict, audit


def stop_spark(spark_sess, config_dict, audit):
    spark_logger = spark_logging.Log4j(spark_sess)
    end_time = datetime.now(pytz.utc)
    audit.etl_end_time = end_time
    audit.etl_status = config_dict['etl_status_completed']

    s3 = S3Utils(spark_sess, config_dict)

    bucket_name = config_dict['s3_etl_runs_bucket']
    etl_job_name = config_dict['etl_job_name']
    key = config_dict['s3_etl_runs_audit_key'] + etl_job_name + "/" + spark_logger.app_id + "_" \
          + end_time.strftime(config_dict['audit_file_suffix']) + ".csv"

    s3.write_object_as_csv(audit, bucket_name, key)

    spark_sess.stop()
    return None
