# SNEWS Kafka Pipeline

A local development environment for translating **Legacy SNEWS 160-byte binary GCN packets** to modern **GCN Unified JSON format**, using Docker-based Kafka for air-gapped testing.

## What This Does

This pipeline bridges the gap between NASA's legacy binary socket protocol and modern JSON-based messaging:

```
┌─────────────────────┐     ┌──────────────┐     ┌─────────────────────┐
│  Legacy GCN Packet  │ ──▶ │  Transformer │ ──▶ │  GCN Unified JSON   │
│  (160 bytes binary) │     │              │     │  (Kafka message)    │
└─────────────────────┘     └──────────────┘     └─────────────────────┘
```

**Key workflow:**

1. Parse 160-byte binary packets using `struct` module
2. Extract bitflags for detector participation and notice types
3. Convert TJD/SOD timestamps to ISO 8601
4. Serialize to validated JSON via Pydantic
5. Publish to Kafka for downstream consumers

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

### Kafka's Role in This Pipeline

| Component                  | Purpose                                                  |
| -------------------------- | -------------------------------------------------------- |
| **Broker**                 | Message queue that stores and distributes alerts         |
| **Topic** (`snews-alerts`) | Channel where SNEWS notices are published                |
| **Producer**               | Publishes transformed JSON messages to the topic         |
| **Consumer**               | Subscribes to the topic and receives alerts in real-time |

### Local Development with Docker

This project uses a **local Docker-based Kafka** instead of connecting to NASA's servers:

```yaml
# docker-compose.yml runs Kafka in KRaft mode (no Zookeeper)
docker compose up -d # Starts Kafka on localhost:9092
```

**Benefits of local Kafka:**

- ✅ **No credentials needed** - Uses PLAINTEXT auth, no NASA Client ID/Secret
- ✅ **Air-gapped safe** - Test alerts can't leak to the astronomy community
- ✅ **Real mechanics** - Tests actual Kafka serialization and network transport
- ✅ **One-line switch** - Change `KAFKA_BOOTSTRAP_SERVERS` to go live

### Going to Production

When you're ready to connect to NASA GCN:

```bash
# .env
KAFKA_BOOTSTRAP_SERVERS=kafka.gcn.nasa.gov:9092
GCN_CLIENT_ID=your-client-id
GCN_CLIENT_SECRET=your-client-secret
GCN_CLIENT_SECRET=your-client-secret
```

### Real vs. Test Notices

In this local pipeline, the difference between a "REAL" and "TEST" notice is **purely data**, not network routing. Both travel over the same local Kafka topic.

| Feature               | REAL Notice                                | TEST Notice                               |
| --------------------- | ------------------------------------------ | ----------------------------------------- |
| **Bitflag**           | `trigger_id` Bit 1 = `0`                   | `trigger_id` Bit 1 = `1`                  |
| **Meaning**           | Actual neutrino detection                  | Scheduled system health check             |
| **Downstream Action** | Telescopes **slew immediately** to targets | Telescopes **log the event** but stay put |
| **How to Send**       | `producer.send_sample(is_test=False)`      | `producer.send_sample(is_test=True)`      |

> [!NOTE]
> When developing locally, you can send "fake real" notices to test your parsing logic. Since the network is air-gapped, there is no risk of accidentally triggering real-world observatories.

---

## Legacy GCN SNEWS Schema Integration

### Binary Packet Format (Type 149)

GCN socket packets are **40 × 4-byte unsigned integers** (160 bytes total) in network byte order (big-endian). This implementation parses the SNEWS-specific packet type 149 as defined in the [GCN Socket Packet Definition](https://gcn.gsfc.nasa.gov/sock_pkt_def_doc.html).

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

The `trigger_id` field encodes notice metadata:

```
Bit 0: Sub-type       (0=Individual, 1=Coincidence)
Bit 1: Test flag      (0=Real, 1=Test)
Bit 2: RA/Dec defined (0=Defined, 1=Undefined - no Super-K)
Bit 5: Retraction     (0=No, 1=Yes - not a supernova)
```

### Detector Participation Bitflags

The `misc` field encodes which of the 7 SNEWS detectors participated:

```
Bits 0-3:   Super-Kamiokande (participated, possible, good, override)
Bits 4-7:   LVD
Bits 8-11:  IceCube
Bits 12-15: KamLAND
Bits 16-19: Borexino
Bits 20-23: Daya Bay
Bits 24-27: HALO
```

Each detector has 4 bits:

- **participated**: Detector contributed to this notice
- **possible**: Burst was possibly real
- **good**: Burst was very likely real
- **override**: Human-verified with extremely high confidence

### Timestamp Conversion

Legacy SNEWS uses **Truncated Julian Day (TJD)** + **Seconds of Day (SOD)**:

```python
# TJD epoch: May 24, 1968 (JD 2440000.5)
# TJD 17023 = January 1, 2015
# SOD 78275.83 = 21:44:35.83 UTC

event_time = TJD_EPOCH + timedelta(days=tjd) + timedelta(seconds=sod)
```

### Coordinate Handling

Only **Super-Kamiokande** (water Cherenkov detector) can determine neutrino direction. For events without Super-K participation:

- RA/Dec fields are undefined
- Error radius is set to 360° (full sky)
- JSON output has `ra: null, dec: null`

---

## Output JSON Format

```json
{
  "$schema_version": "1.0",
  "trigger_num": 131,
  "event_time": "2005-05-11T21:44:35.830000Z",
  "event_tjd": 13501,
  "event_sod": 78275.83,
  "coordinates": {
    "ra": 273.34,
    "dec": 44.6,
    "error_radius": 10.0,
    "containment": 68.0,
    "frame": "J2000"
  },
  "fluence": 18,
  "duration": 10.0,
  "notice_type": "TEST",
  "subtype": "COINCIDENCE",
  "is_retraction": false,
  "detectors": {
    "super_k": { "participated": true, "quality": "good" },
    "lvd": { "participated": true, "quality": "good" },
    "icecube": { "participated": true, "quality": "possible" },
    "kamland": { "participated": false, "quality": null },
    "borexino": { "participated": false, "quality": null },
    "daya_bay": { "participated": false, "quality": null },
    "halo": { "participated": false, "quality": null }
  }
}
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
# Transform a sample packet (no Kafka needed)
python -m src.cli transform --sample

# Send a test notice to Kafka
python -m src.cli produce --sample --test

# Consume and display the notice
python -m src.cli consume --count 1
```

---

## CLI Commands

```bash
# Producer
python -m src.cli produce --sample         # Sample REAL notice
python -m src.cli produce --sample --test  # Sample TEST notice
python -m src.cli produce --file data.bin  # Process binary file

# Consumer
python -m src.cli consume                  # Listen forever
python -m src.cli consume --count 5        # Consume N messages

# Transform (no Kafka)
python -m src.cli transform --sample       # Preview JSON output
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
├── cli.py                 # Command-line interface
├── producer.py            # Kafka JSON producer
├── consumer.py            # Kafka consumer + pretty-print
├── transformer.py         # Binary → JSON pipeline
└── schemas/
    ├── legacy_snews.py    # Binary parser + bitflags
    └── gcn_unified.py     # Pydantic JSON model
```

---

## Testing

```bash
pytest tests/ -v
```

---

## References

- [GCN/SNEWS Notices](https://gcn.gsfc.nasa.gov/snews.html) - Notice format and examples
- [GCN Socket Packet Definition](https://gcn.gsfc.nasa.gov/sock_pkt_def_doc.html) - Binary field layout
- [SNEWS Network](http://snews.bnl.gov) - SuperNova Early Warning System
