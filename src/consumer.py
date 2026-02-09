"""
Kafka Consumer for SNEWS Notices

Consumes GCN Unified JSON messages from Kafka topic for validation and debugging.
"""

import json
import logging
import os
from datetime import datetime
from typing import Callable, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from .schemas.gcn_unified import SNEWSNotice


logger = logging.getLogger(__name__)


class SNEWSKafkaConsumer:
    """
    Kafka consumer for SNEWS notices.
    
    Reads and validates SNEWSNotice JSON messages from Kafka.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
        group_id: str = None,
        auto_offset_reset: str = "earliest",
        on_message: Optional[Callable[[SNEWSNotice], None]] = None,
    ):
        """
        Initialize the SNEWS Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka broker address (default from env or localhost:9092)
            topic: Topic to consume from (default from env or snews-alerts)
            group_id: Consumer group ID (default from env or snews-consumer-group)
            auto_offset_reset: Where to start reading ('earliest' or 'latest')
            on_message: Callback for each message (receives SNEWSNotice)
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.getenv("SNEWS_TOPIC", "snews-alerts")
        self.group_id = group_id or os.getenv(
            "KAFKA_CONSUMER_GROUP", "snews-consumer-group"
        )
        self.on_message = on_message
        
        self._consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        
        logger.info(
            f"SNEWS Kafka Consumer initialized: "
            f"servers={self.bootstrap_servers}, topic={self.topic}, group={self.group_id}"
        )
    
    def consume(self, timeout_ms: int = 1000, max_messages: int = None) -> list:
        """
        Consume messages from Kafka.
        
        Args:
            timeout_ms: Poll timeout in milliseconds
            max_messages: Maximum messages to consume (None for unlimited)
            
        Returns:
            List of SNEWSNotice objects
        """
        notices = []
        message_count = 0
        
        try:
            for message in self._consumer:
                try:
                    # Parse and validate the message
                    notice = SNEWSNotice.model_validate(message.value)
                    notices.append(notice)
                    
                    logger.info(
                        f"Received notice: trigger={notice.trigger_num}, "
                        f"type={notice.notice_type.value}, "
                        f"time={notice.event_time.isoformat()}"
                    )
                    
                    # Call message handler if provided
                    if self.on_message:
                        self.on_message(notice)
                    
                    message_count += 1
                    if max_messages and message_count >= max_messages:
                        break
                        
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        
        return notices
    
    def consume_one(self, timeout_ms: int = 5000) -> Optional[SNEWSNotice]:
        """
        Consume a single message.
        
        Args:
            timeout_ms: Poll timeout in milliseconds
            
        Returns:
            SNEWSNotice or None if no message available
        """
        notices = self.consume(timeout_ms=timeout_ms, max_messages=1)
        return notices[0] if notices else None
    
    def pretty_print(self, notice: SNEWSNotice) -> None:
        """
        Pretty-print a SNEWS notice to console.
        
        Args:
            notice: SNEWSNotice to print
        """
        print("\n" + "=" * 60)
        print(f"SNEWS NOTICE - {notice.notice_type.value} {notice.subtype.value}")
        print("=" * 60)
        print(f"Trigger #:     {notice.trigger_num}")
        print(f"Event Time:    {notice.event_time.isoformat()}")
        print(f"TJD/SOD:       {notice.event_tjd} / {notice.event_sod:.2f}")
        print("-" * 60)
        
        if notice.coordinates.ra is not None:
            print(f"RA:            {notice.coordinates.ra:.4f}°")
            print(f"Dec:           {notice.coordinates.dec:.4f}°")
        else:
            print("RA/Dec:        Undefined (no Super-K)")
        
        print(f"Error Radius:  {notice.coordinates.error_radius:.1f}° "
              f"({notice.coordinates.containment:.0f}% containment)")
        print("-" * 60)
        print(f"Fluence:       {notice.fluence} neutrinos")
        print(f"Duration:      {notice.duration:.2f} sec")
        print("-" * 60)
        print("Detectors:")
        
        for detector_name in ["super_k", "lvd", "icecube", "kamland", "borexino", "daya_bay", "halo"]:
            status = getattr(notice.detectors, detector_name)
            if status.participated:
                quality = status.quality.value if status.quality else "unknown"
                print(f"  {detector_name:12s} ✓ ({quality})")
            else:
                print(f"  {detector_name:12s} -")
        
        if notice.is_retraction:
            print("\n⚠️  THIS IS A RETRACTION NOTICE")
        
        print("=" * 60 + "\n")
    
    def close(self) -> None:
        """Close the consumer connection."""
        self._consumer.close()
        logger.info("SNEWS Kafka Consumer closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
