"""
Data Transformer for SNEWS Legacy to GCN JSON

Converts parsed legacy binary packets to modern GCN Unified JSON format.
Handles TJD/SOD to ISO 8601 conversion and bitflag extraction.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional

from .schemas.legacy_snews import (
    LegacySNEWSPacket,
    LegacySNEWSParser,
    DETECTORS,
)
from .schemas.gcn_unified import (
    SNEWSNotice,
    NoticeType,
    NoticeSubtype,
    Coordinates,
    DetectorStatus,
    DetectorStatuses,
    QualityLevel,
)


# TJD (Truncated Julian Day) epoch calculation:
# TJD = JD - 2440000.5
# JD 2440000.5 corresponds to May 24, 1968 00:00:00 UTC
# 
# Verification from GCN docs:
# - TJD 13501 = 2005-05-22 (from EVENT_DATE: 13501 TJD; 140 DOY; 05/05/22)
#   13501 days after May 24, 1968 = 2005-05-22 ✓
# - TJD 17023 = 2015-01-01 (from documentation comment: "TJD=17023 is 01 Jan 2015")
#   17023 days after May 24, 1968 = 2015-01-01 ✓
#
# Note: We use the day BEFORE as epoch since day 0 starts at this date
TJD_EPOCH = datetime(1968, 5, 24, tzinfo=timezone.utc)


def tjd_sod_to_datetime(tjd: int, sod: float) -> datetime:
    """
    Convert Truncated Julian Day and Seconds-of-Day to datetime.
    
    Args:
        tjd: Truncated Julian Day (e.g., 13501)
        sod: Seconds of day (0-86399.99)
        
    Returns:
        datetime in UTC
        
    Example:
        TJD 13501 + SOD 78275.83 -> 2005-05-22T21:44:35.83Z
    """
    # Start from TJD epoch and add days
    date = TJD_EPOCH + timedelta(days=tjd)
    
    # Add seconds of day
    hours = int(sod // 3600)
    remaining = sod % 3600
    minutes = int(remaining // 60)
    seconds = remaining % 60
    
    # Create final datetime
    result = date.replace(
        hour=hours,
        minute=minutes,
        second=int(seconds),
        microsecond=int((seconds % 1) * 1_000_000)
    )
    
    return result


def get_detector_quality(status: dict) -> Optional[QualityLevel]:
    """
    Determine the quality level from detector status flags.
    
    Priority: override > good > possible
    """
    if status.get("override"):
        return QualityLevel.OVERRIDE
    if status.get("good"):
        return QualityLevel.GOOD
    if status.get("possible"):
        return QualityLevel.POSSIBLE
    return None


class SNEWSTransformer:
    """
    Transforms legacy SNEWS binary packets to GCN Unified JSON.
    """
    
    @classmethod
    def from_bytes(cls, data: bytes) -> SNEWSNotice:
        """
        Parse binary data and convert to SNEWSNotice.
        
        Args:
            data: 160-byte binary packet
            
        Returns:
            SNEWSNotice JSON model
        """
        packet = LegacySNEWSParser.parse(data)
        return cls.from_packet(packet)
    
    @classmethod
    def from_packet(cls, packet: LegacySNEWSPacket) -> SNEWSNotice:
        """
        Convert a parsed packet to SNEWSNotice.
        
        Args:
            packet: Parsed LegacySNEWSPacket
            
        Returns:
            SNEWSNotice JSON model
        """
        # Convert TJD/SOD to datetime
        event_time = tjd_sod_to_datetime(packet.event_tjd, packet.event_sod)
        
        # Build coordinates
        coordinates = Coordinates(
            ra=packet.event_ra,
            dec=packet.event_dec,
            error_radius=packet.event_error,
            containment=packet.event_cont,
            frame="J2000",
        )
        
        # Build detector statuses
        detector_data = packet.get_all_detector_statuses()
        detectors = DetectorStatuses(
            super_k=DetectorStatus(
                participated=detector_data["super_k"]["participated"],
                quality=get_detector_quality(detector_data["super_k"]),
            ),
            lvd=DetectorStatus(
                participated=detector_data["lvd"]["participated"],
                quality=get_detector_quality(detector_data["lvd"]),
            ),
            icecube=DetectorStatus(
                participated=detector_data["icecube"]["participated"],
                quality=get_detector_quality(detector_data["icecube"]),
            ),
            kamland=DetectorStatus(
                participated=detector_data["kamland"]["participated"],
                quality=get_detector_quality(detector_data["kamland"]),
            ),
            borexino=DetectorStatus(
                participated=detector_data["borexino"]["participated"],
                quality=get_detector_quality(detector_data["borexino"]),
            ),
            daya_bay=DetectorStatus(
                participated=detector_data["daya_bay"]["participated"],
                quality=get_detector_quality(detector_data["daya_bay"]),
            ),
            halo=DetectorStatus(
                participated=detector_data["halo"]["participated"],
                quality=get_detector_quality(detector_data["halo"]),
            ),
        )
        
        # Determine notice type and subtype
        notice_type = NoticeType.TEST if packet.is_test else NoticeType.REAL
        subtype = NoticeSubtype.COINCIDENCE if packet.is_coincidence else NoticeSubtype.INDIVIDUAL
        
        return SNEWSNotice(
            trigger_num=packet.trigger_num,
            event_time=event_time,
            event_tjd=packet.event_tjd,
            event_sod=packet.event_sod,
            coordinates=coordinates,
            fluence=packet.event_fluence,
            duration=packet.duration,
            notice_type=notice_type,
            subtype=subtype,
            is_retraction=packet.is_retraction,
            detectors=detectors,
            raw_trigger_id=packet.trigger_id,
            raw_misc=packet.misc,
        )
    
    @classmethod
    def to_json(cls, notice: SNEWSNotice, indent: int = 2) -> str:
        """
        Serialize SNEWSNotice to JSON string.
        
        Args:
            notice: SNEWSNotice model
            indent: JSON indentation level
            
        Returns:
            JSON string
        """
        return notice.model_dump_json(indent=indent, by_alias=True)
    
    @classmethod
    def transform_to_json(cls, data: bytes, indent: int = 2) -> str:
        """
        Full pipeline: binary data to JSON string.
        
        Args:
            data: 160-byte binary packet
            indent: JSON indentation level
            
        Returns:
            JSON string
        """
        notice = cls.from_bytes(data)
        return cls.to_json(notice, indent=indent)
