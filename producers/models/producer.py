
"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer,CachedSchemaRegistryClient
from dataclasses import asdict, dataclass, field

logger = logging.getLogger(__name__)

BROKER_URL = ['localhost:9092, localhost:9093, localhost:9094']


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    print('existing_topics:', existing_topics)

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!

        self.broker_properties = {
            #'bootstrap.servers': 'PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093, PLAINTEXT://localhost:9094',
            'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094',
        }

        # TODO: Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient('http://localhost:8081')
        self.producer = AvroProducer(
            self.broker_properties, 
            schema_registry=schema_registry,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
            )
        #print('created AvroProducer')
        #print('existing topics:', Producer.existing_topics)

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
    
    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        
        #print("topic name:", self.topic_name)
        future = client.create_topics(
            [ 
                NewTopic(
                    topic = self.topic_name,
                    num_partitions = self.num_partitions,
                    replication_factor = self.num_replicas,
                    config = {
                        "cleanup.policy": "compact",
                        "compression.type": "lz4",
                        "delete.retention.ms": 100,
                        "file.delete.delay.ms":  100,       
                    }  
                )
            ] 
        )
                
        try: future[0].result()
        except: print('error')
    
    
        
        """
        futures = AdminClient(self.broker_properties).create_topics(
            [
                NewTopic(
                    topic = self.topic_name,
                    num_partitions = self.num_partitions,
                    replication_factor = self.num_replicas,
                    config = self.broker_properties
                )
            ]
        )
        
        print('AdminClient')
        print('futures:', futures )

        for topic, future in futures.items():
            try:
                future.result()
                print('topic created')
            except Exception as e:
                print(f"failed to create topic {self.topic_name}: {e}")
                
        print('prodicer.py create_topic done')
        """
        
        
    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        # TODO: Write cleanup code for the Producer here
        self.producer.flush()
        #
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))



