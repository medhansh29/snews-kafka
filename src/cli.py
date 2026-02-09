"""
CLI Interface for SNEWS Kafka Pipeline

Commands for producing and consuming SNEWS notices.
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


def main():
    """Main CLI entry point."""
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="SNEWS Kafka Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Send a sample test notice
  python -m src.cli produce --sample --test

  # Start consumer
  python -m src.cli consume

  # Transform binary to JSON (no Kafka)
  python -m src.cli transform --sample
        """,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Produce command
    produce_parser = subparsers.add_parser("produce", help="Produce SNEWS notices to Kafka")
    produce_parser.add_argument("--sample", action="store_true", help="Send sample notice")
    produce_parser.add_argument("--test", action="store_true", help="Mark as TEST notice")
    produce_parser.add_argument("--file", type=str, help="Binary file to process")
    produce_parser.set_defaults(func=cmd_produce)
    
    # Consume command
    consume_parser = subparsers.add_parser("consume", help="Consume SNEWS notices from Kafka")
    consume_parser.add_argument("--count", type=int, help="Maximum messages to consume")
    consume_parser.set_defaults(func=cmd_consume)
    
    # Transform command (no Kafka)
    transform_parser = subparsers.add_parser("transform", help="Transform binary to JSON (no Kafka)")
    transform_parser.add_argument("--sample", action="store_true", help="Use sample packet")
    transform_parser.add_argument("--test", action="store_true", help="Mark as TEST notice")
    transform_parser.add_argument("--file", type=str, help="Binary file to transform")
    transform_parser.set_defaults(func=cmd_transform)
    
    args = parser.parse_args()
    setup_logging(args.verbose)
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    args.func(args)


if __name__ == "__main__":
    main()
