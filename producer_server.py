from kafka import KafkaProducer, KafkaAdminClient
import json
import time
from cached_property import cached_property
from typing import Callable, List
from kafka.admin import NewTopic



from kafka_util import JsonSerializer, get_logger

logger = get_logger(__file__)

class ProducerServer(KafkaProducer):

    def __init__(
        self,
        bootstrap_servers: str,
        input_file: str,
        topic_name: str,
        key_serializer: Callable = str.encode,
        value_serializer: Callable = JsonSerializer().serialize,
        num_partitions: int = 3,
        replication_factor: int = 1,
        **conf,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.input_file = input_file
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self.conf: dict = conf
        self.producer = KafkaProducer(
            key_serializer=key_serializer, value_serializer=value_serializer, **conf
        )

    @cached_property
    def client(self) -> KafkaAdminClient:
        bootstrap_servers: List[str] = self.bootstrap_servers.split(",")
        if not self._client:
            self._client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers, client_id=self.conf.get("client_id")
            )
        return self._client

    def topic_exists(self, topic: str) -> bool:
        """Check if the topic exists in the Kafka"""
        cluster_metadata = self.client.list_topics(timeout=5.0)
        topics = cluster_metadata.topics
        return topic in topics

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # TODO: Write code that creates the topic for this producer if it does not already exist on the Kafka Broker
        if self.topic_exists(self.topic_name):
            logger.debug(f"Topic already exists: {self.topic_name}")
            return

        futures = self.client.create_topics(
            [
                NewTopic(
                    self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
            , timeout_ms = 10000
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.debug(f"Topic with name: {topic} created sucessfully")
            except Exception as exc:
                logger.error(f"Failed to create topic with name : {topic}: {exc}")
            finally:
                self.client.close()

    #TODO we're generating a dummy data

    def read_data(self)->dict:
        with open(self.input_file) as json_file:
            return json.load(json_file)

    def generate_data(self):
        data = self.read_data()
        for record in data:
            key = record.get("crime_id")

            logger.debug(f"Message| key={key} | value={record}")
            future = self.producer.send(topic=self.topic_name, key=key, value=record)
            future.add_callback(self.on_suces).add_errback(self.on_error)
            time.sleep(100)

    def close(self):
        """Flush out all buffered messages and close down the producer gracefully"""
        self.producer.flush(timeout=10)
        self.producer.close(timeout=10)

    def on_suces(self, record_metadata):
        logger.debug(
            f"Successful delivered messaged to broker - {record_metadata.topic}[{record_metadata.partition}]:{record_metadata.offset}"
        )

    def on_error(self, exc):
        logger.error(exc)


    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        pass
        