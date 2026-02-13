"""
Kafka Producer for SNEWS 2.0 Messages

Publishes SNEWS2 JSON messages (all 5 tiers) to Kafka.
Uses the same local Docker Kafka infrastructure as the legacy producer.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional, Callable

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .schemas.snews2_messages import (
    SNEWS2MessageBase,
    HeartbeatMessage,
    RetractionMessage,
    CoincidenceTierMessage,
    SignificanceTierMessage,
    TimingTierMessage,
    Tier,
    parse_snews2_message,
)


logger = logging.getLogger(__name__)


class SNEWS2KafkaProducer:
    """
    Kafka producer for SNEWS 2.0 messages.

    Sends validated Pydantic model instances as JSON to the configured topic.
    """

    DEFAULT_TOPIC = "snews2-alerts"

    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
        acks: str = "all",
        retries: int = 3,
        on_success: Optional[Callable] = None,
        on_error: Optional[Callable] = None,
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.getenv("SNEWS2_TOPIC", self.DEFAULT_TOPIC)
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
            f"SNEWS2 Kafka Producer initialized: "
            f"servers={self.bootstrap_servers}, topic={self.topic}"
        )

    def _on_send_success(self, record_metadata):
        logger.info(
            f"Message sent: topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}"
        )
        if self.on_success:
            self.on_success(record_metadata)

    def _on_send_error(self, exc):
        logger.error(f"Send failed: {exc}")
        if self.on_error:
            self.on_error(exc)

    def send_message(self, message: SNEWS2MessageBase, key: str = None) -> None:
        """
        Send a SNEWS2 message to Kafka.

        Args:
            message: Any SNEWS2 message model instance.
            key: Optional message key (defaults to detector_name).
        """
        # Set sent_time if not already set
        if message.sent_time_utc is None:
            message.sent_time_utc = datetime.now(timezone.utc).isoformat()

        message_key = key or message.detector_name
        message_value = message.to_json()

        future = self._producer.send(
            self.topic,
            key=message_key,
            value=message_value,
        )

        future.add_callback(self._on_send_success)
        future.add_errback(self._on_send_error)

    def send_json(self, data: dict, key: str = None) -> SNEWS2MessageBase:
        """
        Parse a JSON dict, validate it, and send to Kafka.

        Args:
            data: Dict with 'tier' field and tier-specific fields.
            key: Optional message key.

        Returns:
            Validated message model.
        """
        message = parse_snews2_message(data)
        self.send_message(message, key=key)
        return message

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout=timeout)

    def close(self) -> None:
        self._producer.close()
        logger.info("SNEWS2 Kafka Producer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        self.close()
        return False


# ---------------------------------------------------------------------------
# Mock data generators for testing
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_sample_heartbeat(is_test: bool = True) -> HeartbeatMessage:
    """Create a sample heartbeat message."""
    return HeartbeatMessage(
        detector_name="Super-K",
        detector_status="ON",
        machine_time_utc=_now_iso(),
        is_test=is_test,
    )


def create_sample_coincidence(is_test: bool = True) -> CoincidenceTierMessage:
    """Create a sample coincidence tier message."""
    return CoincidenceTierMessage(
        detector_name="Super-K",
        neutrino_time_utc=_now_iso(),
        machine_time_utc=_now_iso(),
        p_val=0.07,
        is_test=is_test,
    )


def create_sample_significance(is_test: bool = True) -> SignificanceTierMessage:
    """Create a sample significance tier message."""
    return SignificanceTierMessage(
        detector_name="KamLAND",
        p_values=[0.43, 0.32, 0.09, 0.01],
        t_bin_width_sec=0.005,
        machine_time_utc=_now_iso(),
        p_val=0.05,
        is_test=is_test,
    )


def create_sample_timing(is_test: bool = True) -> TimingTierMessage:
    """Create a sample timing tier message."""
    return TimingTierMessage(
        detector_name="IceCube",
        neutrino_time_utc=_now_iso(),
        start_time_utc=_now_iso(),
        timing_series=[0, 303000, 659236, 1200000, 1850000],
        time_bin_width_ns=None,  # Unbinned: offsets in ns
        machine_time_utc=_now_iso(),
        is_test=is_test,
    )


def create_sample_retraction(is_test: bool = True) -> RetractionMessage:
    """Create a sample retraction message."""
    return RetractionMessage(
        detector_name="Super-K",
        retract_latest_n=1,
        retraction_reason="False trigger from calibration noise",
        machine_time_utc=_now_iso(),
        is_test=is_test,
    )


SAMPLE_GENERATORS = {
    "heartbeat": create_sample_heartbeat,
    "coincidence": create_sample_coincidence,
    "significance": create_sample_significance,
    "timing": create_sample_timing,
    "retraction": create_sample_retraction,
}
