"""
Integration tests for SNEWS Kafka Producer/Consumer.

Requires running Kafka instance (localhost:9092).
"""

import pytest
import time
import uuid
from src.producer import SNEWSKafkaProducer
from src.consumer import SNEWSKafkaConsumer
from src.schemas.gcn_unified import SNEWSNotice

@pytest.mark.integration
class TestKafkaIntegration:
    """Integration tests requiring running Kafka."""
    
    def test_produce_consume_cycle(self):
        """Test full cycle: Produce -> Kafka -> Consume"""
        # Unique topic for this test to avoid interference
        test_topic = f"snews-test-{uuid.uuid4()}"
        
        # 1. Produce a message
        trigger_num = 999999
        with SNEWSKafkaProducer(topic=test_topic) as producer:
            # Create packet manually
            from src.schemas.legacy_snews import LegacySNEWSParser
            data = LegacySNEWSParser.create_mock_packet(
                trigger_num=trigger_num,
                is_test=True
            )
            producer.send_binary(data)
            producer.flush()
        
        # 2. Consume the message
        received_notice = None
        
        # Give Kafka a moment to settle
        time.sleep(1)
        
        with SNEWSKafkaConsumer(
            topic=test_topic, 
            auto_offset_reset="earliest",
            group_id=f"test-group-{uuid.uuid4()}"
        ) as consumer:
            # Try to consume for up to 5 seconds
            received_notices = consumer.consume(timeout_ms=5000, max_messages=1)
            if received_notices:
                received_notice = received_notices[0]
        
        # 3. Verify
        assert received_notice is not None, "Failed to consume message"
        assert isinstance(received_notice, SNEWSNotice)
        assert received_notice.trigger_num == trigger_num
        assert received_notice.notice_type == "TEST"

if __name__ == "__main__":
    # Allow running directly
    pytest.main([__file__, "-v"])
