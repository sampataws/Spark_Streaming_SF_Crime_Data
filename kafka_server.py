from configparser import ConfigParser
from kafka_util import get_logger
from producer_server import ProducerServer
from pathlib import Path




logger = get_logger(__file__)

KAFKA_CONF_FILE = "sf_application_config.ini"
INPUT_DATA= Path(__file__).parents[0] / "police-department-calls-for-service.json"


def load_config() -> ConfigParser:
    config = ConfigParser()
    config.read(KAFKA_CONF_FILE)
    return config

def run_kafka_producer_server(input_file: str, config: ConfigParser) -> ProducerServer:
    # TODO fill in blanks
    producer = ProducerServer(
        input_file=input_file,
        topic_name=config["kafka"].get("topic"),
        bootstrap_servers=config["kafka"].get("bootstrap_servers"),
        client_id=config["kafka"].get("client_id"),
        num_partitions=config["kafka"].getint("num_partitions"),
        replication_factor=config["kafka"].getint("replication_factor"),
    )
    return producer


def feed_data_to_producer(input_file:str):
    logger.info("INPUT FILE PATH: {} ".format(INPUT_DATA))
    config = load_config()
    producer = run_kafka_producer_server(input_file, config)
    logger.info("Creating Kafka Topic")
    producer.create_topic()
    try:
        logger.info("Calling the generate data function to load data from the JSON file")
        producer.generate_data()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        producer.close()


if __name__ == "__main__":
    feed_data_to_producer(INPUT_DATA)