"""
Kafka Producer for SNEWS Notices

Produces GCN Unified JSON messages to Kafka topic.
Configurable for local (Docker) or production (NASA GCN) endpoints.
"""

import json
import logging
import os
from typing import Optional, Callable

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .transformer import SNEWSTransformer
from .schemas.gcn_unified import SNEWSNotice


logger = logging.getLogger(__name__)


class SNEWSKafkaProducer:
    """
    Kafka producer for SNEWS notices.
    
    Sends SNEWSNotice objects as JSON to the configured Kafka topic.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
        acks: str = "all",
        retries: int = 3,
        on_success: Optional[Callable] = None,
        on_error: Optional[Callable] = None,
    ):
        """
        Initialize the SNEWS Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker address (default from env or localhost:9092)
            topic: Topic to produce to (default from env or snews-alerts)
            acks: Acknowledgment level ('all', '1', '0')
            retries: Number of retries on failure
            on_success: Callback on successful send (receives metadata)
            on_error: Callback on error (receives exception)
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.getenv("SNEWS_TOPIC", "snews-alerts")
        self.on_success = on_success
        self.on_error = on_error
        
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks=acks,
            retries=retries,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        
        logger.info(
            f"SNEWS Kafka Producer initialized: "
            f"servers={self.bootstrap_servers}, topic={self.topic}"
        )
    
    def _on_send_success(self, record_metadata):
        """Internal callback for successful sends."""
        logger.info(
            f"Message sent successfully: "
            f"topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}"
        )
        if self.on_success:
            self.on_success(record_metadata)
    
    def _on_send_error(self, exc):
        """Internal callback for send errors."""
        logger.error(f"Failed to send message: {exc}")
        if self.on_error:
            self.on_error(exc)
    
    def send_notice(self, notice: SNEWSNotice, key: str = None) -> None:
        """
        Send a SNEWS notice to Kafka.
        
        Args:
            notice: SNEWSNotice to send
            key: Optional message key (uses trigger_num if not provided)
        """
        message_key = key or str(notice.trigger_num)
        message_value = notice.model_dump(mode="json", by_alias=True)
        
        future = self._producer.send(
            self.topic,
            key=message_key,
            value=message_value,
        )
        
        future.add_callback(self._on_send_success)
        future.add_errback(self._on_send_error)
    
    def send_binary(self, data: bytes, key: str = None) -> SNEWSNotice:
        """
        Transform binary packet and send to Kafka.
        
        Args:
            data: 160-byte legacy SNEWS binary packet
            key: Optional message key
            
        Returns:
            The transformed SNEWSNotice that was sent
        """
        notice = SNEWSTransformer.from_bytes(data)
        self.send_notice(notice, key=key)
        return notice
    
    def send_sample(self, is_test: bool = True) -> SNEWSNotice:
        """
        Send a sample SNEWS notice for testing.
        
        Args:
            is_test: Whether to mark as test notice
            
        Returns:
            The sample SNEWSNotice that was sent
        """
        from .schemas.legacy_snews import LegacySNEWSParser
        
        # Create mock packet matching documented example
        data = LegacySNEWSParser.create_mock_packet(
            trigger_num=131,
            event_tjd=13501,
            event_sod=78275.83,
            event_ra=273.34,
            event_dec=44.60,
            event_fluence=18,
            event_error=10.0,
            event_cont=68.0,
            duration=10.0,
            is_test=is_test,
            is_coincidence=True,
            # Super-K good, LVD good, IceCube possible, Daya Bay possible
            detector_misc=0x00210157,
        )
        
        return self.send_binary(data)
    
    def flush(self, timeout: float = 10.0) -> None:
        """
        Flush pending messages.
        
        Args:
            timeout: Maximum time to wait in seconds
        """
        self._producer.flush(timeout=timeout)
    
    def close(self) -> None:
        """Close the producer connection."""
        self._producer.close()
        logger.info("SNEWS Kafka Producer closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.flush()
        self.close()
        return False
