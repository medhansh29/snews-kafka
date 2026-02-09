"""
GCN Unified JSON Schema for SNEWS Notices

Modern JSON format for SNEWS neutrino transient data,
designed for Kafka transport and interoperability.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class NoticeType(str, Enum):
    """Type of SNEWS notice (real or test)."""
    REAL = "REAL"
    TEST = "TEST"


class NoticeSubtype(str, Enum):
    """Subtype of SNEWS notice."""
    COINCIDENCE = "COINCIDENCE"
    INDIVIDUAL = "INDIVIDUAL"


class QualityLevel(str, Enum):
    """Quality level for detector observations."""
    POSSIBLE = "possible"
    GOOD = "good"
    OVERRIDE = "override"


class Coordinates(BaseModel):
    """
    Celestial coordinates for the event.
    
    Only defined when Super-Kamiokande participates (the only
    detector capable of directional determination).
    """
    ra: Optional[float] = Field(
        None,
        ge=0,
        lt=360,
        description="Right Ascension in degrees (J2000), 0.0001° precision"
    )
    dec: Optional[float] = Field(
        None,
        ge=-90,
        le=90,
        description="Declination in degrees (J2000), 0.0001° precision"
    )
    error_radius: float = Field(
        ...,
        ge=0,
        le=360,
        description="Error circle radius in degrees"
    )
    containment: float = Field(
        ...,
        ge=0,
        le=100,
        description="Containment percentage for error radius"
    )
    frame: str = Field(
        default="J2000",
        description="Coordinate reference frame"
    )


class DetectorStatus(BaseModel):
    """Status of a single detector's contribution."""
    participated: bool = Field(
        ...,
        description="Whether the detector contributed to this notice"
    )
    quality: Optional[QualityLevel] = Field(
        None,
        description="Quality level of the detection (possible, good, override)"
    )


class DetectorStatuses(BaseModel):
    """Participation status for all SNEWS network detectors."""
    super_k: DetectorStatus = Field(..., description="Super-Kamiokande")
    lvd: DetectorStatus = Field(..., description="Large Volume Detector")
    icecube: DetectorStatus = Field(..., description="IceCube Neutrino Observatory")
    kamland: DetectorStatus = Field(..., description="KamLAND")
    borexino: DetectorStatus = Field(..., description="Borexino")
    daya_bay: DetectorStatus = Field(..., description="Daya Bay")
    halo: DetectorStatus = Field(..., description="HALO")


class SNEWSNotice(BaseModel):
    """
    GCN Unified JSON format for SNEWS notices.
    
    This represents a supernova neutrino transient detection from
    the SNEWS (SuperNova Early Warning System) network.
    """
    
    # Schema metadata
    schema_version: str = Field(
        default="1.0",
        alias="$schema_version",
        description="Schema version"
    )
    
    # Event identification
    trigger_num: int = Field(
        ...,
        ge=0,
        description="Unique trigger serial number assigned by SNEWS"
    )
    
    # Timing
    event_time: datetime = Field(
        ...,
        description="Event time in ISO 8601 format (UTC)"
    )
    
    # Legacy timing (for reference)
    event_tjd: int = Field(
        ...,
        description="Truncated Julian Day (17023 = 01 Jan 2015)"
    )
    event_sod: float = Field(
        ...,
        ge=0,
        lt=86400,
        description="Seconds of day (UT)"
    )
    
    # Location
    coordinates: Coordinates = Field(
        ...,
        description="Celestial coordinates (if available)"
    )
    
    # Event properties
    fluence: int = Field(
        ...,
        ge=0,
        description="Total neutrino count"
    )
    duration: float = Field(
        ...,
        ge=0,
        description="Event duration in seconds"
    )
    
    # Notice metadata
    notice_type: NoticeType = Field(
        ...,
        description="Notice type (REAL or TEST)"
    )
    subtype: NoticeSubtype = Field(
        ...,
        description="Notice subtype (COINCIDENCE or INDIVIDUAL)"
    )
    is_retraction: bool = Field(
        default=False,
        description="True if this retracts a previous notice"
    )
    
    # Detector information
    detectors: DetectorStatuses = Field(
        ...,
        description="Participation status of all SNEWS detectors"
    )
    
    # Raw data (for debugging/reference)
    raw_trigger_id: Optional[int] = Field(
        None,
        description="Original binary trigger_id bitfield"
    )
    raw_misc: Optional[int] = Field(
        None,
        description="Original binary misc bitfield"
    )

    class Config:
        """Pydantic configuration."""
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "$schema_version": "1.0",
                "trigger_num": 131,
                "event_time": "2005-05-22T21:44:35.830000Z",
                "event_tjd": 13501,
                "event_sod": 78275.83,
                "coordinates": {
                    "ra": 273.34,
                    "dec": 44.60,
                    "error_radius": 10.0,
                    "containment": 68.0,
                    "frame": "J2000"
                },
                "fluence": 18,
                "duration": 10.0,
                "notice_type": "REAL",
                "subtype": "COINCIDENCE",
                "is_retraction": False,
                "detectors": {
                    "super_k": {"participated": True, "quality": "good"},
                    "lvd": {"participated": True, "quality": "good"},
                    "icecube": {"participated": True, "quality": "possible"},
                    "kamland": {"participated": False, "quality": None},
                    "borexino": {"participated": False, "quality": None},
                    "daya_bay": {"participated": True, "quality": "possible"},
                    "halo": {"participated": False, "quality": None}
                }
            }
        }
