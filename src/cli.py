"""
CLI Interface for SNEWS Kafka Pipeline

Commands for producing and consuming both legacy SNEWS and SNEWS2 notices.
"""

import argparse
import logging
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Legacy SNEWS commands
# ---------------------------------------------------------------------------

def cmd_produce(args):
    """Handle produce command."""
    from src.producer import SNEWSKafkaProducer
    from src.schemas.legacy_snews import LegacySNEWSParser
    
    with SNEWSKafkaProducer() as producer:
        if args.sample:
            print(f"Sending sample {'TEST' if args.test else 'REAL'} notice...")
            notice = producer.send_sample(is_test=args.test)
            producer.flush()
            print(f"✓ Sent notice: trigger={notice.trigger_num}, type={notice.notice_type.value}")
            print(f"  Event time: {notice.event_time.isoformat()}")
            if notice.coordinates.ra is not None:
                print(f"  Location: RA={notice.coordinates.ra:.4f}°, Dec={notice.coordinates.dec:.4f}°")
            else:
                print("  Location: Undefined (no Super-K)")
        elif args.file:
            print(f"Reading binary packets from: {args.file}")
            with open(args.file, "rb") as f:
                data = f.read()
            
            # Process in 160-byte chunks
            packet_size = 160
            count = 0
            for i in range(0, len(data), packet_size):
                chunk = data[i:i+packet_size]
                if len(chunk) == packet_size:
                    notice = producer.send_binary(chunk)
                    count += 1
                    print(f"  Sent notice #{count}: trigger={notice.trigger_num}")
            
            producer.flush()
            print(f"✓ Sent {count} notices")
        else:
            # Interactive mode: send sample
            print("No input specified. Sending sample TEST notice...")
            notice = producer.send_sample(is_test=True)
            producer.flush()
            print(f"✓ Sent test notice: trigger={notice.trigger_num}")


def cmd_consume(args):
    """Handle consume command."""
    from src.consumer import SNEWSKafkaConsumer
    
    def on_message(notice):
        consumer.pretty_print(notice)
    
    print(f"Starting consumer (Ctrl+C to stop)...")
    print(f"Topic: {os.getenv('SNEWS_TOPIC', 'snews-alerts')}")
    print("-" * 60)
    
    with SNEWSKafkaConsumer(on_message=on_message) as consumer:
        try:
            if args.count:
                notices = consumer.consume(max_messages=args.count)
                print(f"\n✓ Consumed {len(notices)} notices")
            else:
                consumer.consume()  # Infinite loop until Ctrl+C
        except KeyboardInterrupt:
            print("\n\n✓ Consumer stopped")


def cmd_transform(args):
    """Handle transform command (binary to JSON without Kafka)."""
    from src.transformer import SNEWSTransformer
    from src.schemas.legacy_snews import LegacySNEWSParser
    
    if args.sample:
        # Use sample packet
        data = LegacySNEWSParser.create_mock_packet(
            is_test=args.test,
            is_coincidence=True,
        )
        print("Transforming sample packet:\n")
    elif args.file:
        with open(args.file, "rb") as f:
            data = f.read(160)
        print(f"Transforming first packet from {args.file}:\n")
    else:
        print("No input specified. Using sample packet:\n")
        data = LegacySNEWSParser.create_mock_packet(is_test=True)
    
    json_output = SNEWSTransformer.transform_to_json(data, indent=2)
    print(json_output)


# ---------------------------------------------------------------------------
# SNEWS2 commands
# ---------------------------------------------------------------------------

SNEWS2_TIERS = ["heartbeat", "coincidence", "significance", "timing", "retraction"]


def cmd_snews2_produce(args):
    """Handle snews2-produce command."""
    from src.snews2_producer import SNEWS2KafkaProducer, SAMPLE_GENERATORS

    tier = args.tier
    if tier not in SAMPLE_GENERATORS:
        print(f"Unknown tier: {tier}. Choose from: {', '.join(SNEWS2_TIERS)}")
        sys.exit(1)

    with SNEWS2KafkaProducer() as producer:
        msg = SAMPLE_GENERATORS[tier](is_test=args.test)
        producer.send_message(msg)
        producer.flush()

        test_label = " [TEST]" if msg.is_test else ""
        tier_display = msg.tier.value if hasattr(msg.tier, 'value') else msg.tier
        print(f"✓ Sent SNEWS2 {tier_display}{test_label}")
        print(f"  Detector:  {msg.detector_name}")
        print(f"  UUID:      {msg.uuid[:12]}...")


def cmd_snews2_consume(args):
    """Handle snews2-consume command."""
    from src.snews2_consumer import SNEWS2KafkaConsumer

    def on_message(msg):
        SNEWS2KafkaConsumer.pretty_print(msg)

    topic = os.getenv("SNEWS2_TOPIC", "snews2-alerts")
    print(f"Subscribing to SNEWS2 alerts (Ctrl+C to stop)...")
    print(f"Topic: {topic}")
    print("-" * 60)

    with SNEWS2KafkaConsumer(on_message=on_message) as consumer:
        try:
            if args.count:
                messages = consumer.consume(max_messages=args.count)
                print(f"\n✓ Consumed {len(messages)} SNEWS2 messages")
            else:
                consumer.consume()
        except KeyboardInterrupt:
            print("\n\n✓ Consumer stopped")


def cmd_snews2_transform(args):
    """Handle snews2-transform: show sample SNEWS2 JSON for a tier."""
    import json
    from src.snews2_producer import SAMPLE_GENERATORS

    tier = args.tier
    if tier not in SAMPLE_GENERATORS:
        print(f"Unknown tier: {tier}. Choose from: {', '.join(SNEWS2_TIERS)}")
        sys.exit(1)

    msg = SAMPLE_GENERATORS[tier](is_test=args.test)
    tier_display = msg.tier.value if hasattr(msg.tier, 'value') else msg.tier
    print(f"SNEWS2 {tier_display} sample message:\n")
    print(json.dumps(msg.to_json(), indent=2))


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

def main():
    """Main CLI entry point."""
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="SNEWS Kafka Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples (Legacy SNEWS):
  python -m src.cli produce --sample --test
  python -m src.cli consume
  python -m src.cli transform --sample

Examples (SNEWS2):
  python -m src.cli snews2-produce --tier coincidence --test
  python -m src.cli snews2-consume --count 5
  python -m src.cli snews2-transform --tier timing
        """,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # --- Legacy SNEWS commands ---
    produce_parser = subparsers.add_parser("produce", help="Produce legacy SNEWS notices to Kafka")
    produce_parser.add_argument("--sample", action="store_true", help="Send sample notice")
    produce_parser.add_argument("--test", action="store_true", help="Mark as TEST notice")
    produce_parser.add_argument("--file", type=str, help="Binary file to process")
    produce_parser.set_defaults(func=cmd_produce)
    
    consume_parser = subparsers.add_parser("consume", help="Consume legacy SNEWS notices from Kafka")
    consume_parser.add_argument("--count", type=int, help="Maximum messages to consume")
    consume_parser.set_defaults(func=cmd_consume)
    
    transform_parser = subparsers.add_parser("transform", help="Transform legacy binary to JSON (no Kafka)")
    transform_parser.add_argument("--sample", action="store_true", help="Use sample packet")
    transform_parser.add_argument("--test", action="store_true", help="Mark as TEST notice")
    transform_parser.add_argument("--file", type=str, help="Binary file to transform")
    transform_parser.set_defaults(func=cmd_transform)
    
    # --- SNEWS2 commands ---
    s2_produce = subparsers.add_parser("snews2-produce", help="Produce SNEWS2 alerts to Kafka")
    s2_produce.add_argument("--tier", type=str, default="coincidence",
                            choices=SNEWS2_TIERS, help="Message tier to send")
    s2_produce.add_argument("--test", action="store_true", help="Mark as TEST")
    s2_produce.set_defaults(func=cmd_snews2_produce)

    s2_consume = subparsers.add_parser("snews2-consume", help="Subscribe to SNEWS2 alerts from Kafka")
    s2_consume.add_argument("--count", type=int, help="Maximum messages to consume")
    s2_consume.set_defaults(func=cmd_snews2_consume)

    s2_transform = subparsers.add_parser("snews2-transform", help="Show sample SNEWS2 JSON (no Kafka)")
    s2_transform.add_argument("--tier", type=str, default="coincidence",
                              choices=SNEWS2_TIERS, help="Message tier to display")
    s2_transform.add_argument("--test", action="store_true", help="Mark as TEST")
    s2_transform.set_defaults(func=cmd_snews2_transform)
    
    args = parser.parse_args()
    setup_logging(args.verbose)
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    args.func(args)


if __name__ == "__main__":
    main()

