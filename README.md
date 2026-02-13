# SNEWS Kafka Pipeline

A local development environment for translating **Legacy SNEWS 160-byte binary GCN packets** and **SNEWS 2.0 JSON messages** to modern formats, using Docker-based Kafka for air-gapped testing.

## What This Does

This pipeline supports two SNEWS generations:

```
Legacy SNEWS:
  160-byte binary ──▶ Transformer ──▶ GCN Unified JSON ──▶ Kafka (snews-alerts)

SNEWS 2.0:
  JSON (5 tiers) ──▶ Validation ──▶ Kafka (snews2-alerts)
```

---

## The Role of Kafka

### Why Kafka?

NASA's **GCN (General Coordinates Network)** uses Apache Kafka as its modern transport layer for astronomical alerts. When a supernova neutrino burst is detected by SNEWS, the alert flows through Kafka to observatories worldwide within seconds.

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────────────┐
│   SNEWS     │ ──▶ │  NASA GCN Kafka │ ──▶ │  Observatories      │
│  Detectors  │     │  (Broker)       │     │  (Consumers)        │
└─────────────┘     └─────────────────┘     └─────────────────────┘
```

### Kafka Topics

| Topic           | Format           | Source       |
| --------------- | ---------------- | ------------ |
| `snews-alerts`  | GCN Unified JSON | Legacy SNEWS |
| `snews2-alerts` | SNEWS2 JSON      | SNEWS 2.0    |

### Local Development with Docker

```bash
docker compose up -d  # Starts Kafka on localhost:9092 (KRaft mode)
```

**Benefits:** No credentials needed, air-gapped safe, real Kafka mechanics, one-line switch to production.

### Going to Production

```bash
# .env
KAFKA_BOOTSTRAP_SERVERS=kafka.gcn.nasa.gov:9092
GCN_CLIENT_ID=your-client-id
GCN_CLIENT_SECRET=your-client-secret
```

### Real vs. Test Notices

Both legacy and SNEWS2 support test/real distinction:

| Feature               | REAL Notice                     | TEST Notice              |
| --------------------- | ------------------------------- | ------------------------ |
| **Legacy bitflag**    | `trigger_id` Bit 1 = `0`        | `trigger_id` Bit 1 = `1` |
| **SNEWS2 field**      | `is_test: false`                | `is_test: true`          |
| **Downstream Action** | Telescopes **slew immediately** | Telescopes **log only**  |

> [!NOTE]
> Local Kafka is air-gapped. You can send "fake real" notices safely.

---

## SNEWS 2.0 Integration

SNEWS2 uses **JSON messages** over hopskotch-flavored Kafka (via [snews_pt](https://github.com/SNEWS2/SNEWS_Publishing_Tools)), with 5 distinct message tiers:

### Message Tiers

| Tier                 | Key Fields                                               | Purpose                        |
| -------------------- | -------------------------------------------------------- | ------------------------------ |
| **Heartbeat**        | `detector_status` (ON/OFF)                               | Detector health monitoring     |
| **Retraction**       | `retract_message_uuid` or `retract_latest_n`             | Withdraw a previous alert      |
| **CoincidenceTier**  | `neutrino_time_utc`, `p_val`                             | Multi-detector coincidence     |
| **SignificanceTier** | `p_values[]`, `t_bin_width_sec`                          | Statistical burst significance |
| **TimingTier**       | `neutrino_time_utc`, `timing_series[]`, `start_time_utc` | Precise arrival timing         |

**Common fields** (all tiers): `uuid`, `tier`, `detector_name`, `sent_time_utc`, `machine_time_utc`, `is_pre_sn`, `is_test`, `is_firedrill`, `meta`, `schema_version`

### SNEWS2 Sample Message (CoincidenceTier)

```json
{
  "tier": "CoincidenceTier",
  "detector_name": "Super-K",
  "neutrino_time_utc": "2025-01-15T14:30:00.123456+00:00",
  "p_val": 0.07,
  "is_test": true,
  "schema_version": "0.2"
}
```

### Publishing SNEWS2 Alerts

```bash
# Send a sample coincidence alert (test)
python -m src.cli snews2-produce --tier coincidence --test

# Send a timing tier alert
python -m src.cli snews2-produce --tier timing --test

# Other tiers: heartbeat, significance, retraction
python -m src.cli snews2-produce --tier heartbeat
```

### Subscribing to SNEWS2 Alerts

```bash
# Subscribe and display all incoming alerts (Ctrl+C to stop)
python -m src.cli snews2-consume

# Consume a specific number of messages
python -m src.cli snews2-consume --count 5
```

<!-- TODO: Document subscribing to live SNEWS2 alerts via hopskotch/SCiMMA
     when production hop credentials are available. The snews_pt Subscriber
     class (snews_pt.snews_sub.Subscriber) connects to hop broker topics
     and saves alerts as JSON files. Our local implementation mirrors this
     pattern using plain Kafka consumers. -->

### Preview SNEWS2 JSON (No Kafka)

```bash
# Show JSON output for any tier
python -m src.cli snews2-transform --tier timing
python -m src.cli snews2-transform --tier significance --test
```

### Schema Comparison: Legacy vs SNEWS2

| Feature            | Legacy SNEWS            | SNEWS 2.0                            |
| ------------------ | ----------------------- | ------------------------------------ |
| **Wire format**    | 160-byte binary         | JSON                                 |
| **Transport**      | GCN socket → Kafka      | Hopskotch Kafka                      |
| **Timestamps**     | TJD + SOD               | ISO 8601 (nanosecond)                |
| **Coordinates**    | RA/Dec (Super-K only)   | Not included (aggregated separately) |
| **Detectors**      | 7 detectors in bitflags | Named detectors (expandable)         |
| **Test flag**      | Bitflag in `trigger_id` | `is_test` boolean                    |
| **Message types**  | 1 (type 149)            | 5 tiers                              |
| **Schema version** | Implicit                | Explicit (`schema_version`)          |

---

## Legacy GCN SNEWS Schema

### Binary Packet Format (Type 149)

GCN socket packets are **40 × 4-byte unsigned integers** (160 bytes total) in network byte order (big-endian).

| Position | Field           | Encoding | Description                     |
| -------- | --------------- | -------- | ------------------------------- |
| 0        | `pkt_type`      | uint32   | Always 149 for SNEWS            |
| 4        | `trigger_num`   | uint32   | Unique event serial number      |
| 5        | `event_tjd`     | uint32   | Truncated Julian Day            |
| 6        | `event_sod`     | uint32   | Seconds-of-day × 100            |
| 7        | `event_ra`      | uint32   | RA × 10000 (degrees)            |
| 8        | `event_dec`     | int32    | Dec × 10000 (degrees, signed)   |
| 9        | `event_fluence` | uint32   | Neutrino count                  |
| 10       | `event_error`   | uint32   | Error radius × 10000            |
| 11       | `event_cont`    | uint32   | Containment % × 100             |
| 12       | `duration`      | uint32   | Duration × 100 (seconds)        |
| 18       | `trigger_id`    | uint32   | Notice type bitflags            |
| 19       | `misc`          | uint32   | Detector participation bitflags |

### Trigger ID Bitflags

```
Bit 0: Sub-type       (0=Individual, 1=Coincidence)
Bit 1: Test flag      (0=Real, 1=Test)
Bit 2: RA/Dec defined (0=Defined, 1=Undefined - no Super-K)
Bit 5: Retraction     (0=No, 1=Yes - not a supernova)
```

### Detector Participation Bitflags

```
Bits 0-3:   Super-Kamiokande    Bits 16-19: Borexino
Bits 4-7:   LVD                 Bits 20-23: Daya Bay
Bits 8-11:  IceCube             Bits 24-27: HALO
Bits 12-15: KamLAND
```

Each detector has 4 bits: participated, possible, good, override.

### Timestamp Conversion

```python
# TJD epoch: May 24, 1968 (JD 2440000.5)
event_time = TJD_EPOCH + timedelta(days=tjd) + timedelta(seconds=sod)
```

---

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka

```bash
docker compose up -d
```

### 3. Test the Pipeline

```bash
# Legacy SNEWS
python -m src.cli transform --sample
python -m src.cli produce --sample --test
python -m src.cli consume --count 1

# SNEWS2
python -m src.cli snews2-transform --tier coincidence
python -m src.cli snews2-produce --tier coincidence --test
python -m src.cli snews2-consume --count 1
```

---

## CLI Commands

```bash
# Legacy SNEWS
python -m src.cli produce --sample --test  # Sample TEST notice
python -m src.cli produce --file data.bin  # Process binary file
python -m src.cli consume --count 5        # Consume N messages
python -m src.cli transform --sample       # Preview JSON output

# SNEWS2
python -m src.cli snews2-produce --tier coincidence --test
python -m src.cli snews2-produce --tier timing
python -m src.cli snews2-consume                           # Subscribe forever
python -m src.cli snews2-consume --count 5                 # Consume N messages
python -m src.cli snews2-transform --tier significance     # Preview JSON
```

---

## Configuration

Copy `.env.example` to `.env`:

```bash
# Local development
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Production (NASA GCN)
# KAFKA_BOOTSTRAP_SERVERS=kafka.gcn.nasa.gov:9092
```

---

## Project Structure

```
src/
├── cli.py                 # CLI (legacy + SNEWS2 commands)
├── producer.py            # Legacy Kafka producer
├── consumer.py            # Legacy Kafka consumer
├── transformer.py         # Binary → JSON pipeline
├── snews2_producer.py     # SNEWS2 Kafka producer + mock generators
├── snews2_consumer.py     # SNEWS2 Kafka consumer + pretty-print
└── schemas/
    ├── legacy_snews.py    # Binary parser + bitflags
    ├── gcn_unified.py     # Legacy Pydantic JSON model
    └── snews2_messages.py # SNEWS2 Pydantic models (5 tiers)
tests/
├── test_transformer.py    # Legacy parser unit tests (13 tests)
├── test_snews2.py         # SNEWS2 schema unit tests (29 tests)
└── test_integration.py    # Kafka end-to-end tests
```

---

## Testing

```bash
pytest tests/ -v  # 42 tests total
```

---

## References

- [GCN/SNEWS Notices](https://gcn.gsfc.nasa.gov/snews.html) - Legacy notice format
- [GCN Socket Packet Definition](https://gcn.gsfc.nasa.gov/sock_pkt_def_doc.html) - Binary field layout
- [SNEWS Network](http://snews.bnl.gov) - SuperNova Early Warning System
- [snews_pt](https://github.com/SNEWS2/SNEWS_Publishing_Tools) - SNEWS 2.0 Publishing Tools
- [snews-data-formats](https://github.com/SNEWS2/snews-data-formats) - SNEWS2 data models and schema
