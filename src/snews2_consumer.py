"""
Kafka Consumer for SNEWS 2.0 Messages

Subscribes to SNEWS2 alerts, validates incoming JSON against Pydantic models,
and provides tier-aware pretty-printing.
"""

import json
import logging
import os
from typing import Callable, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from .schemas.snews2_messages import (
    SNEWS2MessageBase,
    Tier,
    parse_snews2_message,
)


logger = logging.getLogger(__name__)


class SNEWS2KafkaConsumer:
    """
    Kafka consumer for SNEWS 2.0 messages.

    Reads JSON messages, validates them against the appropriate tier model,
    and optionally pretty-prints them.
    """

    DEFAULT_TOPIC = "snews2-alerts"

    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
        group_id: str = None,
        auto_offset_reset: str = "earliest",
        on_message: Optional[Callable[[SNEWS2MessageBase], None]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.getenv("SNEWS2_TOPIC", self.DEFAULT_TOPIC)
        self.group_id = group_id or os.getenv(
            "KAFKA_CONSUMER_GROUP", "snews2-consumer-group"
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
            f"SNEWS2 Consumer initialized: "
            f"servers={self.bootstrap_servers}, topic={self.topic}, group={self.group_id}"
        )

    def consume(self, timeout_ms: int = 1000, max_messages: int = None) -> list:
        """
        Consume messages from Kafka.

        Returns:
            List of validated SNEWS2 message model instances.
        """
        messages = []
        count = 0

        try:
            for record in self._consumer:
                try:
                    msg = parse_snews2_message(record.value)
                    messages.append(msg)

                    logger.info(
                        f"Received: tier={msg.tier}, "
                        f"detector={msg.detector_name}, "
                        f"test={msg.is_test}"
                    )

                    if self.on_message:
                        self.on_message(msg)

                    count += 1
                    if max_messages and count >= max_messages:
                        break

                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")

        return messages

    def consume_one(self, timeout_ms: int = 5000) -> Optional[SNEWS2MessageBase]:
        results = self.consume(timeout_ms=timeout_ms, max_messages=1)
        return results[0] if results else None

    @staticmethod
    def pretty_print(msg: SNEWS2MessageBase) -> None:
        """Tier-aware pretty-print of a SNEWS2 message."""
        tier = msg.tier if isinstance(msg.tier, str) else msg.tier.value
        test_label = " [TEST]" if msg.is_test else ""
        firedrill_label = " [FIREDRILL]" if msg.is_firedrill else ""
        pre_sn_label = " [PRE-SN]" if msg.is_pre_sn else ""

        print("\n" + "=" * 60)
        print(f"SNEWS2 {tier}{test_label}{firedrill_label}{pre_sn_label}")
        print("=" * 60)
        print(f"Detector:      {msg.detector_name}")
        print(f"UUID:          {msg.uuid[:12]}...")
        if msg.machine_time_utc:
            print(f"Machine Time:  {msg.machine_time_utc}")
        if msg.sent_time_utc:
            print(f"Sent Time:     {msg.sent_time_utc}")
        print("-" * 60)

        # Tier-specific fields
        if tier == Tier.HEARTBEAT or tier == "Heartbeat":
            print(f"Status:        {msg.detector_status}")

        elif tier == Tier.RETRACTION or tier == "Retraction":
            if msg.retract_message_uuid:
                print(f"Retract UUID:  {msg.retract_message_uuid}")
            else:
                print(f"Retract Last:  {msg.retract_latest_n} message(s)")
            if msg.retraction_reason:
                print(f"Reason:        {msg.retraction_reason}")

        elif tier == Tier.COINCIDENCE_TIER or tier == "CoincidenceTier":
            print(f"Neutrino Time: {msg.neutrino_time_utc}")
            if msg.p_val is not None:
                print(f"p-value:       {msg.p_val}")

        elif tier == Tier.SIGNIFICANCE_TIER or tier == "SignificanceTier":
            print(f"p-values:      {msg.p_values}")
            print(f"Bin Width:     {msg.t_bin_width_sec} sec")
            if msg.p_val is not None:
                print(f"Combined p:    {msg.p_val}")

        elif tier == Tier.TIMING_TIER or tier == "TimingTier":
            print(f"Neutrino Time: {msg.neutrino_time_utc}")
            print(f"Start Time:    {msg.start_time_utc}")
            n = len(msg.timing_series)
            if n <= 5:
                print(f"Timing Data:   {msg.timing_series}")
            else:
                print(f"Timing Data:   [{msg.timing_series[0]}, ..., {msg.timing_series[-1]}] ({n} entries)")
            if msg.time_bin_width_ns:
                print(f"Bin Width:     {msg.time_bin_width_ns} ns (binned)")
            else:
                print(f"Format:        Unbinned (offsets in ns)")
            if msg.detection_channel:
                print(f"Channel:       {msg.detection_channel}")

        if msg.meta:
            print(f"Metadata:      {msg.meta}")

        print("=" * 60 + "\n")

    def close(self) -> None:
        self._consumer.close()
        logger.info("SNEWS2 Consumer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
