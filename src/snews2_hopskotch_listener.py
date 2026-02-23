"""
Hopskotch listener for SNEWS2 alerts to GCN Kafka publisher.
"""

import json
import logging
import os
import sys

from kafka import KafkaProducer

from .schemas.snews2_messages import parse_snews2_message
from .schemas.snews2_gcn_schema import transform_snews2_to_gcn

logger = logging.getLogger(__name__)


class SNEWS2HopskotchListener:
    """
    Listens for alerts using SNEWS_PT and republishes them to local Kafka.
    """
    DEFAULT_GCN_TOPIC = "snews2-gcn-alerts"
    
    def __init__(
        self,
        gcn_topic: str = None,
        bootstrap_servers: str = None,
    ):
        self.gcn_topic = gcn_topic or os.getenv("SNEWS2_GCN_TOPIC", self.DEFAULT_GCN_TOPIC)
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        logger.info(f"Initialized KafkaProducer for topic {self.gcn_topic} at {self.bootstrap_servers}")

    def process_message(self, message_data: dict):
        """Processes a single hopskotch JSON payload, mapping it and publishing it to Kafka."""
        try:
            # 1. Validate against SNEWS2 models
            snews2_msg = parse_snews2_message(message_data)
            logger.info(f"Received from Hopskotch: tier={snews2_msg.tier}, detector={snews2_msg.detector_name}")
            
            # 2. Transform to GCN schema
            gcn_notice = transform_snews2_to_gcn(snews2_msg)
            
            # 3. Publish to local Kafka
            # Use detector name as key to ensure ordering for a given detector
            key = gcn_notice.detector_name
            
            future = self.producer.send(
                self.gcn_topic, 
                key=key, 
                value=gcn_notice.model_dump(mode="json")
            )
            future.get(timeout=10) # Wait for confirmation
            logger.info(f"Successfully published SNEWS2 alert to Kafka topic '{self.gcn_topic}'")
            
            return gcn_notice
            
        except Exception as e:
            logger.error(f"Error processing SNEWS2 message: {e}")
            return None


def run_hopskotch_plugin():
    """
    Entrypoint for when this script is run as a snews_pt plugin:
    snews_pt subscribe --no-firedrill -p src/snews2_hopskotch_listener.py
    
    snews_pt passes the json file path as sys.argv[1].
    """
    if len(sys.argv) < 2:
        print("Error: path to JSON file not provided by snews_pt plugin hook.")
        sys.exit(1)
        
    json_path = sys.argv[1]
    
    try:
        with open(json_path, "r") as f:
            message_data = json.load(f)
            
        listener = SNEWS2HopskotchListener()
        listener.process_message(message_data)
        
    except Exception as e:
        print(f"Failed to process hopskotch plugin message: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # If run directly by snews_pt subscriber plugin system
    run_hopskotch_plugin()
