"""
spark_logging
~~~~~~~

This module contains a class that wraps the log4j object instantiated
by the active SparkContext, enabling Log4j logging for PySpark using.
"""


class Log4j(object):
    """Wrapper class for Log4j JVM object.

    :param spark: SparkSession object.
    """
    # Variable for auditing, same logger instance is available in all the modules
    # Adding any key-value, in any module would add to the same instance variable
    audit_dict = {}

    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        self.app_id = conf.get('spark.app.id')
        self.app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + self.app_name + ' ' + self.app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log an error.

        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log an warning.

        :param: Error message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.

        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None

    def debug(self, message):
        """Log debug message.

        :param: Debug message to write to log
        :return: None
        """
        self.logger.debug(message)
        return None

    def set_audit(self, key, value):
        self.audit_dict.update({key: value})
        return None

    def get_audit(self, key, value):
        return self.audit_dict
