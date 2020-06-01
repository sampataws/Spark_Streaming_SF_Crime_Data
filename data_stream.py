import logging
import os
from configparser import ConfigParser

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from pathlib import Path
from kafka_util import get_logger
from pyspark.sql.streaming import StreamingQuery

import findspark
findspark.init('/usr/local/Cellar/spark-2.4.5-bin-hadoop2.7/')


logger = get_logger(__file__)

RADIO_CD_JSON_FILEPATH=Path(__file__).parents[0] / "radio_code.json"
KAFKA_CONF_FILE = "sf_application_config.ini"


# TODO Create a schema for incoming resources
POLICE_DEPT_CALLS_FOR_SERVICE_SCHEMA = StructType(
    [
        StructField("crime_id", StringType(), False),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("call_date", TimestampType(), True),
        StructField("offense_date", TimestampType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True),
    ]
)

RADIO_CODE_SCHEMA = StructType(
    [
        StructField("disposition_code", StringType(), False),
        StructField("description", StringType(), True),
    ]
)

def load_config() -> ConfigParser:
    config = ConfigParser()
    config.read(KAFKA_CONF_FILE)
    return config

def run_spark_job(spark:SparkSession,config,radio_code_json_filepath: str):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    logger.info("Kafka bootstapserver details -> {}".format(config["kafka"].get("bootstrap_servers")))
    logger.info("topic subscribe -> {}".format(config["kafka"].get("topic")))

    df:DataFrame = (
        spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", config["kafka"].get("bootstrap_servers"))\
        .option("subscribe", config["kafka"].get("topic"))\
        .option("startingOffsets","earliest")\
        .option("maxOffsetsPerTrigger",200)\
        .option("stopGracefullyOnShutdown", "true")\
        .load()
    )
    logger.info("Printing schema for topic - {}".format(config["kafka"].get("topic")))
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    logger.info("Converting topic value to String")
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df.select(
        psf.from_json(psf.col("value"), POLICE_DEPT_CALLS_FOR_SERVICE_SCHEMA).alias("crime_data_dataframe")
    ).select("crime_data_dataframe.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select("original_crime_type_name",  "call_date_time","disposition").withWatermark("call_date_time", "60 minutes")

    # count the number of original crime type
    agg_df = distinct_table.groupBy(
        "original_crime_type_name", psf.window("call_date_time", "30 minutes")
    ).count()

    agg_df.printSchema()


    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df\
            .writeStream\
            .queryName("aggregate_query_for_crime_data")\
            .outputMode("Complete")\
            .format("console")\
            .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    radio_code_df:DataFrame = (
        spark.read.option("multiline", "true")
            .schema(RADIO_CODE_SCHEMA)
            .json(radio_code_json_filepath)
    )

    # TODO rename disposition_code column to disposition
    new_radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(new_radio_code_df, on="disposition", how="inner").select(
        "original_crime_type_name", "disposition", "description", "count"
    )

    join_query: StreamingQuery = (
        join_query.writeStream
            .trigger(processingTime="5 seconds")
            .format("console")
            .option("truncate", "false")
            .start()
    )


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark Session successfully create with object name spark")
    logger.info("Loading configration from config file {}".format(KAFKA_CONF_FILE))
    configuration = load_config()

    run_spark_job(spark,configuration,RADIO_CD_JSON_FILEPATH)

    spark.stop()
