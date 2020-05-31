from kafka import KafkaProducer, KafkaAdminClient
import json
import time
from cached_property import cached_property
from typing import Callable, List
from kafka.errors import TopicAlreadyExistsError
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
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers, client_id=self.conf.get("client_id")
        )
        return admin_client

    def create_topic(self):
        new_topic = NewTopic(
            name=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
        )

        try:
            self.client.create_topics([new_topic])
        except TopicAlreadyExistsError:
            logger.info(f"Topic already exists: {new_topic.name}")
        finally:
            self.client.close()


    def read_data(self)->dict:
        with open(self.input_file) as json_file:
            return json.load(json_file)

    def generate_data(self):
        data = self.read_data()
        for rec in data:
            key = rec.get("crime_id")
            logger.debug("Message| key={} | value={}".format(key,rec))
            self.producer.send(topic=self.topic_name, key=key, value=rec)
            time.sleep(100)

    def close(self):
        """Flush out all buffered messages and close down the producer gracefully"""
        self.producer.flush(timeout=10)
        self.producer.close(timeout=10)

        