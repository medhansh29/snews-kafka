# SNEWS Kafka Pipeline

A local development environment for translating **Legacy SNEWS 160-byte binary GCN packets** and **SNEWS 2.0 JSON messages** to modern formats, using Docker-based Kafka for air-gapped testing.

## What This Does

This pipeline supports two SNEWS generations:

```
Legacy SNEWS:
  160-byte binary ──▶ Transformer ──▶ GCN Unified JSON ──▶ Kafka (snews-alerts)

SNEWS 2.0 (via Hopskotch):
  SCiMMA Hopskotch ──▶ snews_pt Listener ──▶ GCN Unified Schema ──▶ Kafka (snews2-gcn-alerts)
```

> [!NOTE]
> Currently, we lack publishing credentials for NASA GCN. Therefore, the final step—publishing to GCN—is mocked by sending the unified JSON schema to a local Docker Kafka topic (`snews2-gcn-alerts`).

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

### How Tier Messaging Works

Unlike legacy SNEWS (a single 160-byte binary packet), SNEWS2 splits alerts into **5 specialized tiers**, each serving a different purpose in the supernova detection workflow.

#### 1. Heartbeat — "I'm alive"

The simplest tier. Detectors periodically send heartbeats to confirm they're online and operational. If heartbeats stop arriving, the network knows something is wrong.

```json
{ "tier": "Heartbeat", "detector_name": "Super-K", "detector_status": "ON" }
```

#### 2. CoincidenceTier — "Multiple detectors saw something!"

The **core alert**. When two or more detectors independently detect a neutrino burst within a narrow time window, the SNEWS server generates a coincidence alert.

Key field: **`p_val`** — the statistical probability that this coincidence is a false alarm. Lower = more likely a real supernova.

```json
{
  "tier": "CoincidenceTier",
  "detector_name": "Super-K",
  "neutrino_time_utc": "2025-01-15T14:30:00.123456+00:00",
  "p_val": 0.07,
  "is_test": true
}
```

#### 3. SignificanceTier — "Here's how significant it is"

Provides **statistical significance over time bins**. Instead of a single p-value, it sends an array of `p_values[]` — each representing the false-alarm probability in successive time windows of width `t_bin_width_sec`. This lets scientists see whether the signal is growing stronger (real supernova) or fading (noise).

#### 4. TimingTier — "Exactly when the neutrinos arrived"

The most data-rich tier. Contains **precise arrival times of individual neutrinos** as nanosecond offsets from a reference `start_time_utc`. Critical for:

- **Triangulating the supernova's sky position** (comparing arrival times across detectors at different locations on Earth)
- **Studying neutrino physics** (oscillation, mass ordering)

Supports two modes:

- **Unbinned**: Raw nanosecond offsets (`[0, 303000, 659236, ...]`)
- **Binned**: Histogram counts with a `time_bin_width_ns`

#### 5. Retraction — "Never mind, false alarm"

Allows a detector to **withdraw** a previous message — either by UUID (`retract_message_uuid`) or by count (`retract_latest_n`). Includes an optional `retraction_reason`.

### Detection Flow

A typical supernova detection sequence:

```
Time ──────────────────────────────────────────────────────▶

Detector A: Heartbeat ── Heartbeat ── [burst!] ── TimingTier
Detector B: Heartbeat ── Heartbeat ── [burst!] ── TimingTier
                                          │
                                          ▼
                                 CoincidenceTier (server-generated)
                                          │
                                          ▼
                                 SignificanceTier (p-values over time)
                                          │
                                 (if false alarm)
                                          ▼
                                    Retraction
```

### Common Fields

Every tier shares a base set of fields:

| Field                      | Purpose                                         |
| -------------------------- | ----------------------------------------------- |
| `uuid`                     | Unique message ID                               |
| `detector_name`            | Which detector sent it                          |
| `is_test` / `is_firedrill` | Flags to prevent accidental real alerts         |
| `machine_time_utc`         | When the detector's clock generated the message |
| `schema_version`           | For forward compatibility (`"0.2"` currently)   |

### Tier Summary

| Tier                 | Key Fields                                               | Purpose                        |
| -------------------- | -------------------------------------------------------- | ------------------------------ |
| **Heartbeat**        | `detector_status` (ON/OFF)                               | Detector health monitoring     |
| **Retraction**       | `retract_message_uuid` or `retract_latest_n`             | Withdraw a previous alert      |
| **CoincidenceTier**  | `neutrino_time_utc`, `p_val`                             | Multi-detector coincidence     |
| **SignificanceTier** | `p_values[]`, `t_bin_width_sec`                          | Statistical burst significance |
| **TimingTier**       | `neutrino_time_utc`, `timing_series[]`, `start_time_utc` | Precise arrival timing         |

### Unified GCN Schema for SNEWS 2.0

Because GCN expects a unified format rather than 5 distinct tier types, the pipeline automatically wraps received Hopskotch messages into a standard `SNEWS2GCNNotice` envelope. This is the schema that will be ultimately published to NASA GCN:

```json
{
  "schema_version": "1.0",
  "gcn_notice_type": "SNEWS2_ALERT",
  "is_test": true,
  "is_firedrill": true,
  "snews2_tier": "CoincidenceTier",
  "snews2_message_uuid": "...",
  "detector_name": "Super-K",
  "event_time_utc": "2025-01-15T14:30:00.123456+00:00",
  "tier_data": {
    "p_val": 0.07
  }
}
```

_Note: Tier-specific fields (like `p_val`, `timing_series`, or `detector_status`) are dynamically nested inside the `tier_data` object._

### Listening to SNEWS2 Alerts (Hopskotch)

This is the primary ingestion method for SNEWS2. We use `snews_pt` to listen to SCiMMA and route it through our GCN transformer.

```bash
# Subscribe to the test/firedrill network and mock publish to local Kafka
python -m src.cli snews2-hopskotch-listen --firedrill

# Subscribe to the LIVE production network (requires Hopskotch credentials in ~/.config/hop/auth.toml)
python -m src.cli snews2-hopskotch-listen --no-firedrill
```

### Producing Mock SNEWS2 Alerts (Local Testing)

If you wish to bypass Hopskotch entirely for local testing, you can produce mock SNEWS2 alerts directly into Kafka:

```bash
# Send a sample coincidence alert (test)
python -m src.cli snews2-produce --tier coincidence --test
```

### Subscribing to SNEWS2 Alerts

```bash
# Subscribe and display all incoming alerts (Ctrl+C to stop)
python -m src.cli snews2-consume

# Consume a specific number of messages
python -m src.cli snews2-consume --count 5
```

<!-- Note: The hopskotch listener acts as the primary subscriber to SNEWS announcements.
     The `snews2-consume` command is just an internal viewer to verify messages made it into the local Kafka mock. -->

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

### 2. Setup Hopskotch Authentication (For SNEWS2 Live Data)

```bash
hop auth add
```

_(Provide your SCiMMA Username, Password, and `kafka.scimma.org` as the hostname)._

### 3. Start Local Kafka (GCN Mock)

```bash
docker compose up -d
```

### 4. Test the Pipeline

```bash
# Legacy SNEWS
python -m src.cli transform --sample
python -m src.cli produce --sample --test
python -m src.cli consume --count 1

# SNEWS2 (Listen -> Mock GCN)
python -m src.cli snews2-hopskotch-listen --firedrill
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
python -m src.cli snews2-hopskotch-listen --firedrill      # Listen to Hopskotch & publish to GCN mock
python -m src.cli snews2-hopskotch-listen --no-firedrill   # Listen to LIVE Hopskotch
python -m src.cli snews2-produce --tier coincidence --test # Local testing mock producer
python -m src.cli snews2-consume                           # View messages arriving in GCN mock
python -m src.cli snews2-transform --tier significance     # Preview SNEWS2 generated JSON
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
├── snews2_hopskotch_listener.py # Hopskotch -> GCN mock publisher
├── snews2_producer.py     # SNEWS2 Kafka producer + mock generators
├── snews2_consumer.py     # SNEWS2 Kafka consumer + pretty-print
└── schemas/
    ├── legacy_snews.py    # Binary parser + bitflags
    ├── gcn_unified.py     # Legacy Pydantic JSON model
    ├── snews2_gcn_schema.py # Custom SNEWS2 -> GCN envelope schema
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
