"""
Legacy SNEWS Binary Schema Parser

Parses the 160-byte binary GCN socket packet (type=149) as documented at:
https://gcn.gsfc.nasa.gov/sock_pkt_def_doc.html
https://gcn.gsfc.nasa.gov/snews.html

The packet contains fields for SNEWS neutrino transient detection data.
"""

import struct
from dataclasses import dataclass
from enum import IntFlag
from typing import Optional


class TriggerIdFlags(IntFlag):
    """
    Bitflags for the trigger_id field (32-bit).
    
    Bit 0: Sub-type (0=Individual, 1=Coincidence)
    Bit 1: Test flag (0=Real, 1=Test)
    Bit 2: RA/Dec undefined (0=Defined, 1=Undefined)
    Bit 5: Retract flag (0=No, 1=Yes - not a supernova)
    Bit 30: Test submission (internal use)
    """
    SUBTYPE_COINCIDENCE = 1 << 0
    IS_TEST = 1 << 1
    RADEC_UNDEFINED = 1 << 2
    RETRACT = 1 << 5
    TEST_SUBMIT = 1 << 30


class DetectorFlags(IntFlag):
    """
    Bitflags for the misc field (32-bit) indicating detector participation.
    
    Each detector has 4 bits:
    - participated: detector contributed to the notice
    - possible: burst was possibly real
    - good: burst was very likely real
    - override: human-checked, extremely high confidence
    """
    # Super-Kamiokande (bits 0-3)
    SUPER_K_PARTICIPATED = 1 << 0
    SUPER_K_POSSIBLE = 1 << 1
    SUPER_K_GOOD = 1 << 2
    SUPER_K_OVERRIDE = 1 << 3
    
    # LVD (bits 4-7)
    LVD_PARTICIPATED = 1 << 4
    LVD_POSSIBLE = 1 << 5
    LVD_GOOD = 1 << 6
    LVD_OVERRIDE = 1 << 7
    
    # IceCube (bits 8-11)
    ICECUBE_PARTICIPATED = 1 << 8
    ICECUBE_POSSIBLE = 1 << 9
    ICECUBE_GOOD = 1 << 10
    ICECUBE_OVERRIDE = 1 << 11
    
    # KamLAND (bits 12-15)
    KAMLAND_PARTICIPATED = 1 << 12
    KAMLAND_POSSIBLE = 1 << 13
    KAMLAND_GOOD = 1 << 14
    KAMLAND_OVERRIDE = 1 << 15
    
    # Borexino (bits 16-19)
    BOREXINO_PARTICIPATED = 1 << 16
    BOREXINO_POSSIBLE = 1 << 17
    BOREXINO_GOOD = 1 << 18
    BOREXINO_OVERRIDE = 1 << 19
    
    # Daya Bay (bits 20-23)
    DAYA_BAY_PARTICIPATED = 1 << 20
    DAYA_BAY_POSSIBLE = 1 << 21
    DAYA_BAY_GOOD = 1 << 22
    DAYA_BAY_OVERRIDE = 1 << 23
    
    # HALO (bits 24-27)
    HALO_PARTICIPATED = 1 << 24
    HALO_POSSIBLE = 1 << 25
    HALO_GOOD = 1 << 26
    HALO_OVERRIDE = 1 << 27


# Detector names and their bit offsets
DETECTORS = [
    ("super_k", 0),
    ("lvd", 4),
    ("icecube", 8),
    ("kamland", 12),
    ("borexino", 16),
    ("daya_bay", 20),
    ("halo", 24),
]


@dataclass
class LegacySNEWSPacket:
    """
    Represents a parsed legacy SNEWS binary packet.
    
    Based on GCN socket packet type=149 (SNEWS_Position).
    All GCN packets are 160 bytes (40 x 4-byte longs).
    """
    # Header fields (common to all GCN packets)
    pkt_type: int           # Packet type (149 for SNEWS)
    pkt_sernum: int         # Serial number
    pkt_hop_cnt: int        # Hop count
    
    # SNEWS-specific fields
    trigger_num: int        # Unique trigger serial number
    event_tjd: int          # Truncated Julian Day (17023 = 01 Jan 2015)
    event_sod: float        # UT seconds-of-day (2 decimal places)
    event_ra: Optional[float]   # RA in degrees (0.0001° precision), None if undefined
    event_dec: Optional[float]  # Dec in degrees (0.0001° precision), None if undefined
    event_fluence: int      # Neutrino count
    event_error: float      # Error radius in degrees
    event_cont: float       # Containment percentage
    duration: float         # Event duration in seconds
    trigger_id: int         # Bitflags (subtype, test, etc.)
    misc: int               # Detector participation bitflags
    
    @property
    def is_test(self) -> bool:
        """Check if this is a test notice."""
        return bool(self.trigger_id & TriggerIdFlags.IS_TEST)
    
    @property
    def is_coincidence(self) -> bool:
        """Check if this is a coincidence (vs individual) notice."""
        return bool(self.trigger_id & TriggerIdFlags.SUBTYPE_COINCIDENCE)
    
    @property
    def is_retraction(self) -> bool:
        """Check if this is a retraction notice."""
        return bool(self.trigger_id & TriggerIdFlags.RETRACT)
    
    @property
    def radec_undefined(self) -> bool:
        """Check if RA/Dec coordinates are undefined."""
        return bool(self.trigger_id & TriggerIdFlags.RADEC_UNDEFINED)
    
    def get_detector_status(self, detector_name: str) -> dict:
        """
        Get the participation status for a specific detector.
        
        Returns dict with keys: participated, possible, good, override
        """
        for name, offset in DETECTORS:
            if name == detector_name:
                return {
                    "participated": bool(self.misc & (1 << offset)),
                    "possible": bool(self.misc & (1 << (offset + 1))),
                    "good": bool(self.misc & (1 << (offset + 2))),
                    "override": bool(self.misc & (1 << (offset + 3))),
                }
        raise ValueError(f"Unknown detector: {detector_name}")
    
    def get_all_detector_statuses(self) -> dict:
        """Get participation status for all detectors."""
        return {name: self.get_detector_status(name) for name, _ in DETECTORS}


class LegacySNEWSParser:
    """
    Parser for legacy SNEWS 160-byte binary packets.
    
    GCN packets are 40 x 4-byte network-order (big-endian) longs.
    The exact field positions are based on the GCN socket packet definition.
    """
    
    # Packet size in bytes
    PACKET_SIZE = 160
    
    # Field positions (in 4-byte long units)
    # Based on GCN socket packet definition for type 149
    PKT_TYPE_POS = 0
    PKT_SERNUM_POS = 1
    PKT_HOP_CNT_POS = 2
    TRIGGER_NUM_POS = 4
    EVENT_TJD_POS = 5
    EVENT_SOD_POS = 6      # Stored as centi-seconds (SOD * 100)
    EVENT_RA_POS = 7       # Stored as centi-degrees (RA * 10000)
    EVENT_DEC_POS = 8      # Stored as centi-degrees (Dec * 10000)
    EVENT_FLUENCE_POS = 9
    EVENT_ERROR_POS = 10   # Stored as centi-degrees
    EVENT_CONT_POS = 11    # Stored as centi-percent
    DURATION_POS = 12      # Stored as centi-seconds
    TRIGGER_ID_POS = 18
    MISC_POS = 19
    
    @classmethod
    def parse(cls, data: bytes) -> LegacySNEWSPacket:
        """
        Parse a 160-byte binary packet into a LegacySNEWSPacket.
        
        Args:
            data: 160 bytes of binary data in network byte order
            
        Returns:
            LegacySNEWSPacket with parsed fields
            
        Raises:
            ValueError: If packet size is incorrect or type is not 149
        """
        if len(data) != cls.PACKET_SIZE:
            raise ValueError(f"Expected {cls.PACKET_SIZE} bytes, got {len(data)}")
        
        # Unpack all 40 longs (big-endian, network order)
        longs = struct.unpack(">40I", data)
        
        pkt_type = longs[cls.PKT_TYPE_POS]
        if pkt_type != 149:
            raise ValueError(f"Expected packet type 149 (SNEWS), got {pkt_type}")
        
        trigger_id = longs[cls.TRIGGER_ID_POS]
        
        # Check if RA/Dec are undefined
        radec_undefined = bool(trigger_id & TriggerIdFlags.RADEC_UNDEFINED)
        
        # Parse RA/Dec (stored as value * 10000)
        raw_ra = longs[cls.EVENT_RA_POS]
        raw_dec = longs[cls.EVENT_DEC_POS]
        
        # Handle signed declination (stored as unsigned, needs sign extension)
        if raw_dec > 0x7FFFFFFF:
            raw_dec = raw_dec - 0x100000000
        
        event_ra = None if radec_undefined else raw_ra / 10000.0
        event_dec = None if radec_undefined else raw_dec / 10000.0
        
        return LegacySNEWSPacket(
            pkt_type=pkt_type,
            pkt_sernum=longs[cls.PKT_SERNUM_POS],
            pkt_hop_cnt=longs[cls.PKT_HOP_CNT_POS],
            trigger_num=longs[cls.TRIGGER_NUM_POS],
            event_tjd=longs[cls.EVENT_TJD_POS],
            event_sod=longs[cls.EVENT_SOD_POS] / 100.0,
            event_ra=event_ra,
            event_dec=event_dec,
            event_fluence=longs[cls.EVENT_FLUENCE_POS],
            event_error=longs[cls.EVENT_ERROR_POS] / 10000.0,
            event_cont=longs[cls.EVENT_CONT_POS] / 100.0,
            duration=longs[cls.DURATION_POS] / 100.0,
            trigger_id=trigger_id,
            misc=longs[cls.MISC_POS],
        )
    
    @classmethod
    def create_mock_packet(
        cls,
        trigger_num: int = 131,
        event_tjd: int = 13501,
        event_sod: float = 78275.83,
        event_ra: Optional[float] = 273.34,
        event_dec: Optional[float] = 44.60,
        event_fluence: int = 18,
        event_error: float = 10.0,
        event_cont: float = 68.0,
        duration: float = 10.0,
        is_test: bool = False,
        is_coincidence: bool = True,
        detector_misc: int = 0x0057,  # Super-K good, LVD good, IceCube possible
    ) -> bytes:
        """
        Create a mock 160-byte binary packet for testing.
        
        Args:
            trigger_num: Unique trigger identifier
            event_tjd: Truncated Julian Day
            event_sod: Seconds of day
            event_ra: Right Ascension (None for undefined)
            event_dec: Declination (None for undefined)
            event_fluence: Neutrino count
            event_error: Error radius in degrees
            event_cont: Containment percentage
            duration: Event duration in seconds
            is_test: Whether this is a test notice
            is_coincidence: Whether this is a coincidence (vs individual)
            detector_misc: Detector participation bitflags
            
        Returns:
            160-byte binary packet in network byte order
        """
        # Build trigger_id flags
        trigger_id = 0
        if is_coincidence:
            trigger_id |= TriggerIdFlags.SUBTYPE_COINCIDENCE
        if is_test:
            trigger_id |= TriggerIdFlags.IS_TEST
        if event_ra is None or event_dec is None:
            trigger_id |= TriggerIdFlags.RADEC_UNDEFINED
        
        # Initialize 40 longs with zeros
        longs = [0] * 40
        
        # Set header fields
        longs[cls.PKT_TYPE_POS] = 149
        longs[cls.PKT_SERNUM_POS] = 1
        longs[cls.PKT_HOP_CNT_POS] = 0
        
        # Set SNEWS fields
        longs[cls.TRIGGER_NUM_POS] = trigger_num
        longs[cls.EVENT_TJD_POS] = event_tjd
        longs[cls.EVENT_SOD_POS] = int(event_sod * 100)
        
        # Handle RA/Dec
        if event_ra is not None:
            longs[cls.EVENT_RA_POS] = int(event_ra * 10000)
        if event_dec is not None:
            raw_dec = int(event_dec * 10000)
            if raw_dec < 0:
                raw_dec = raw_dec + 0x100000000
            longs[cls.EVENT_DEC_POS] = raw_dec
        
        longs[cls.EVENT_FLUENCE_POS] = event_fluence
        longs[cls.EVENT_ERROR_POS] = int(event_error * 10000)
        longs[cls.EVENT_CONT_POS] = int(event_cont * 100)
        longs[cls.DURATION_POS] = int(duration * 100)
        longs[cls.TRIGGER_ID_POS] = trigger_id
        longs[cls.MISC_POS] = detector_misc
        
        return struct.pack(">40I", *longs)
