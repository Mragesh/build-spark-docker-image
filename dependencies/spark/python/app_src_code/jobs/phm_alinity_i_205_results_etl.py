"""
phm_alinity_i_205_results_etl.py
~~~~~~~~~~

This Python module contains the Spark ETL job definition.
It can be submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/global_config.json \
    jobs/phm_alinity_i_205_results_etl.py

where packages.zip contains Python modules required by ETL job,
which need to be made available to each executor process on every node
in the cluster; global_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, phm_alinity_i_205_results_etl.py contains the Spark application
to be executed by a driver process on the Spark master node.
"""
import sys

# Add package path to execution
sys.path.append("/opt/spark/python/packages")

from pyspark.sql.functions import *
import uuid
from dependencies import InstrumentLogFileUtils
from dependencies.S3Utils import S3Utils
from dependencies.spark import start_spark, stop_spark
from pyspark.sql.types import StringType


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger, config and audit
    spark, logger, config_dict, audit = start_spark(
        app_name='PHM_alinity_i_205_results'
    )

    # log that main ETL job is starting
    logger.info('etl_job is up-and-running')

    # execute ETL pipeline

    data = extract_data(spark, config_dict)
    data_transformed = transform_data(data)

    partition_list = data_transformed.agg(collect_set('transaction_date')).collect()[0][0]
    logger.debug("Patitions to dedup: ")
    logger.debug(str(partition_list))

    cleansed_bucket = config_dict['etl_cleansed_bucket']
    cleansed_key_prefix = config_dict['etl_cleansed_key'] + "/transaction_date="
    s3_utils = S3Utils(spark, config_dict)
    paths_list = []

    for partition_suffix in partition_list:
        if s3_utils.is_key_prefix_empty(cleansed_bucket, cleansed_key_prefix + str(partition_suffix)):
            paths_list.extend(config_dict['s3a_prefix'] + cleansed_bucket + cleansed_key_prefix
                              + str(partition_suffix) + "/*")
            logger.debug("Patitions to dedup: " + str(paths_list))

    if paths_list:
        data_cleansed = spark.read.format("parquet").load(paths_list)
        data_deduped = deduplicate_data(data_transformed, data_cleansed)
        load_data(data_deduped, config_dict['s3a_prefix'] + config_dict['etl_cleansed_bucket']
                  + "/" + config_dict['etl_cleansed_key'])
    else:
        load_data(data_transformed, config_dict['s3a_prefix'] + config_dict['etl_cleansed_bucket']
                  + "/" + config_dict['etl_cleansed_key'])

    # log the success and terminate Spark application
    logger.info('test_etl_job is finished')
    stop_spark(spark, config_dict, audit)
    return None


def extract_data(spark, config_dict):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :param config_dict:
    :return: Spark DataFrame.
    """
    log_files = InstrumentLogFileUtils.InstrumentLogFileUtils(spark, config_dict)
    files_list = log_files.get_files_to_be_processed()
    df = spark.read.format("avro").load(files_list)

    return df


def transform_data(df):
    """Transform original dataset.

    :param df: Extracted DataFrame
    :return: Transformed DataFrame.
    """

    df1 = df.withColumn("file_path", input_file_name())
    nested_df = df1.select("module.*", "customerInfo.*", "deviceId", "url", "file_path")

    exploded_df = nested_df.withColumn("data", explode("data"))

    flat_df = exploded_df.select("moduleIdName", "platformTypeName", "moduleType", "moduleName", "platformType",
                                 "productLine", "scmSerialNumber", "installDateLocal", "installDate", "inUse",
                                 "systemSoftwareVersion",
                                 "moduleSerialNumber", "moduleSoftwareVersion", "moduleId", "moduleTypeName",
                                 "installDateUtc", "country",
                                 "customerNumber", "deviceId", "url", "file_path", "data.*")
    # flat_df.printSchema()
    # print("PHM_Trans Total Rows: "+str(flat_df.count()))
    schema_df = flat_df.select(
        flat_df.aimCode.cast("bigint"),
        flat_df.aimSubCode.cast("string"),
        flat_df.aliquotId.cast("bigint"),
        flat_df.assayName.cast("string"),
        flat_df.assayNumber.cast("bigint"),
        flat_df.assayStatus.cast("string"),
        flat_df.assayType.cast("bigint"),
        flat_df.assayTypeName.cast("string"),
        flat_df.assayVersion.cast("bigint"),
        flat_df.calcRefBeforeVoltage.cast("double"),
        flat_df.calcReferenceAfterVoltage.cast("double"),
        flat_df.calcSampleVoltage.cast("double"),
        flat_df.calculatedAbsorbance.cast("double"),
        flat_df.calibrationDate.cast("timestamp"),
        flat_df.calibrationDateLocal.cast("timestamp"),
        flat_df.calibrationDateUtc.cast("timestamp"),
        flat_df.calibrationExpirationDate.cast("timestamp"),
        flat_df.calibrationExpirationDateLocal.cast("timestamp"),
        flat_df.calibrationExpirationDateUtc.cast("timestamp"),
        flat_df.calibrationLotNumber.cast("string"),
        flat_df.clReadings.cast("double"),
        flat_df.controlLevel.cast("string"),
        flat_df.controlLotExpirationDate.cast("timestamp"),
        flat_df.controlLotExpirationDateLocal.cast("timestamp"),
        flat_df.controlLotExpirationDateUtc.cast("timestamp"),
        flat_df.controlLotNumber.cast("string"),
        flat_df.controlMaxRange.cast("double"),
        flat_df.controlMinRange.cast("double"),
        flat_df.controlName.cast("string"),
        flat_df.correctedCount.cast("bigint"),
        flat_df.country.cast("string"),
        flat_df.customerNumber.cast("string"),
        flat_df.cuvetteNumber.cast("bigint"),
        flat_df.dac.cast("bigint"),
        flat_df.dACErrorCode.cast("bigint"),
        flat_df.darkAverage.cast("double"),
        flat_df.darkReadPeakInterval.cast("bigint"),
        flat_df.darkSignalReads.cast("string"),
        flat_df.darkStdDeviation.cast("double"),
        flat_df.darkVariance.cast("double"),
        regexp_extract(flat_df.file_path, r'date=(.+)/hr', 1).cast("date").alias("date_"),
        flat_df.dateTimeStamp.cast("timestamp"),
        flat_df.dateTimeStampLocal.cast("timestamp"),
        flat_df.dateTimeStampUtc.cast("timestamp"),
        flat_df.deviceId.cast("bigint"),
        flat_df.dilutionId.cast("bigint"),
        flat_df.dilutionProtocol.cast("string"),
        flat_df.errorCodeData.cast("string"),
        flat_df.exceptionString.cast("string"),
        flat_df.foregroundPeakInterval.cast("bigint"),
        flat_df.file_path,
        regexp_extract(flat_df.file_path, r'hr=(\d+)', 1).cast("int").alias("hr_"),
        flat_df.ictRefVoltagePostSample.cast("bigint"),
        flat_df.ictRefVoltagePreSample.cast("bigint"),
        flat_df.ictVoltage.cast("double"),
        flat_df.installDate.cast("timestamp"),
        flat_df.installDateLocal.cast("timestamp"),
        flat_df.installDateUtc.cast("timestamp"),
        flat_df.instrumentId.cast("bigint"),
        flat_df.integratedDarkCount.cast("bigint"),
        flat_df.integratedSignalCount.cast("bigint"),
        flat_df.interpretationCutoffType.cast("bigint"),
        flat_df.interpretationCutoffTypeName.cast("string"),
        flat_df.interpretationCutoffValue.cast("double"),
        flat_df.interpretationEditable.cast("bigint"),
        flat_df.interpretationMaxValue.cast("double"),
        flat_df.interpretationMinValue.cast("double"),
        flat_df.interpretationType.cast("bigint"),
        flat_df.interpretationTypeName.cast("string"),
        flat_df.inUse.cast("bigint"),
        flat_df.isAbbottControl.cast("bigint"),
        flat_df.isDerived.cast("bigint"),
        flat_df.kReadings.cast("double"),
        flat_df.loadListName.cast("string"),
        flat_df.maxDarkCountRead.cast("bigint"),
        flat_df.maxForegroundRead.cast("bigint"),
        flat_df.minDarkCountRead.cast("bigint"),
        flat_df.minForegroundRead.cast("bigint"),
        flat_df.moduleId.cast("bigint"),
        flat_df.moduleIdName.cast("string"),
        flat_df.moduleName.cast("string"),
        flat_df.moduleSerialNumber.cast("string"),
        flat_df.moduleSoftwareVersion.cast("string"),
        flat_df.moduleType.cast("string"),
        flat_df.moduleTypeName.cast("string"),
        flat_df.naReadings.cast("double"),
        flat_df.operatorId.cast("string"),
        flat_df.platformType.cast("string"),
        flat_df.platformTypeName.cast("string"),
        flat_df.primaryKey.cast("bigint"),
        flat_df.primaryWavelength.cast("bigint"),
        flat_df.primaryWavelengthReads.cast("string"),
        flat_df.productLine.cast("string"),
        flat_df.rawResult.cast("double"),
        flat_df.reagentExpirationDate.cast("timestamp"),
        flat_df.reagentExpirationDateLocal.cast("timestamp"),
        flat_df.reagentExpirationDateUtc.cast("timestamp"),
        flat_df.reagentMasterLotNumber.cast("string"),
        flat_df.reagentOnboardStability.cast("bigint"),
        flat_df.reagentSerialNumber.cast("string"),
        flat_df.reportedResult.cast("string"),
        flat_df.reportedResultUnit.cast("string"),
        flat_df.resultCodes.cast("string"),
        flat_df.resultComment.cast("string"),
        flat_df.resultFlags.cast("string"),
        flat_df.resultInterpretation.cast("string"),
        flat_df.sampleId.cast("string"),
        flat_df.sampleSourceId.cast("string"),
        flat_df.sampleType.cast("bigint"),
        flat_df.sampleTypeName.cast("string"),
        flat_df.scmSerialNumber.cast("string"),
        flat_df.secondaryWavelength.cast("bigint"),
        flat_df.secondaryWavelengthReads.cast("string"),
        flat_df.signalAverage.cast("double"),
        flat_df.signalReads.cast("string"),
        flat_df.signalStdDeviation.cast("double"),
        flat_df.signalVariance.cast("double"),
        flat_df.systemSoftwareVersion.cast("string"),
        flat_df.testCompletionDate.cast("timestamp"),
        flat_df.testCompletionDateLocal.cast("timestamp"),
        flat_df.testCompletionDateUtc.cast("timestamp"),
        flat_df.testId.cast("bigint"),
        flat_df.testInitiationDate.cast("timestamp"),
        flat_df.testInitiationDateLocal.cast("timestamp"),
        flat_df.testInitiationDateUtc.cast("timestamp"),
        flat_df.testOrderDate.cast("timestamp"),
        flat_df.testOrderDateLocal.cast("timestamp"),
        flat_df.testOrderDateUtc.cast("timestamp"),
        flat_df.url.cast("string")
    )

    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
    transformed_df = schema_df.distinct() \
        .withColumn("transaction_date", to_date("dateTimeStampLocal", 'yyyy-MM-dd')) \
        .withColumn("ale_id", uuid_udf()) \
        .withColumn("ale_transform_time", current_timestamp())

    return transformed_df


def load_data(df, out_path):
    """Collect data locally and write to CSV.

    :param out_path: path to write transformed data
    :param transformed_df: transformed dataframe to save as parquet files
    :return: None
    """
    repart_df = df.repartition(10)
    repart_df.write.parquet(out_path, mode="overwrite",
                            partitionBy="transaction_date", compression='snappy')
    return None


def deduplicate_data(data_transformed, data_cleansed):
    """

    :return:
    """

    data_merged = data_transformed + data_cleansed
    # window = Window.partitionBy('primarykey', 'testid', 'sampleid', 'moduleserialnumber', 'transaction_date')
    # dedupCountdf = data_merged.withColumn('dup_count', count('primarykey').over(window))
    data_deduped = data_merged.dropDuplicates(
        ['primarykey', 'testid', 'sampleid', 'moduleserialnumber', 'transaction_Date'])

    return data_deduped


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

