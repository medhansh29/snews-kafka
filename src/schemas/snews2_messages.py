"""
SNEWS 2.0 Message Schema Models

Pydantic models for the 5 SNEWS2 message tiers, adapted from the
snews-data-formats package (https://github.com/SNEWS2/snews-data-formats).

Message tiers:
  - Heartbeat:        Detector health monitoring (ON/OFF)
  - Retraction:       Withdraw a previous alert
  - CoincidenceTier:  Multi-detector coincidence detection
  - SignificanceTier: Statistical significance of neutrino burst
  - TimingTier:       Precise neutrino arrival timing data

All timestamps use ISO 8601-1:2019 format with nanosecond precision.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Tier(str, Enum):
    """SNEWS2 message tier identifiers."""
    HEARTBEAT = "Heartbeat"
    RETRACTION = "Retraction"
    TIMING_TIER = "TimingTier"
    SIGNIFICANCE_TIER = "SignificanceTier"
    COINCIDENCE_TIER = "CoincidenceTier"


class DetectorStatus(str, Enum):
    """Detector operational status."""
    ON = "ON"
    OFF = "OFF"


class DetectionChannel(str, Enum):
    """Neutrino detection channel."""
    NU_E = "Electron Neutrino"
    NU_E_BAR = "Electron Antineutrino"
    NC = "Neutral Current"
    OTHER = "Other"


# Known SNEWS2 detectors (from snews-data-formats detector registry)
SNEWS2_DETECTORS = [
    "Super-K", "Hyper-K", "LVD", "IceCube", "KamLAND",
    "Borexino", "HALO", "HALO-1kT", "SNO+", "NOvA",
    "DUNE", "MicroBooNE", "SBND", "DS-20K", "XENONnT",
    "JUNO", "KM3NeT", "Baksan", "PandaX-4T",
]

SCHEMA_VERSION = "0.2"


# ---------------------------------------------------------------------------
# Base models
# ---------------------------------------------------------------------------

class SNEWS2MessageBase(BaseModel):
    """
    Common fields shared by all SNEWS2 message tiers.

    Mirrors the MessageBase from snews-data-formats with simplified
    validation (no numpy/hop dependencies).
    """
    model_config = ConfigDict(
        validate_assignment=True,
        use_enum_values=True,
    )

    id: Optional[str] = Field(
        default=None,
        description="Human-readable message ID (auto-generated if omitted)",
    )

    uuid: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the message",
    )

    tier: Tier = Field(
        ...,
        description="Message tier type",
    )

    sent_time_utc: Optional[str] = Field(
        default=None,
        description="Time the message was sent (ISO 8601)",
    )

    machine_time_utc: Optional[str] = Field(
        default=None,
        description="Time of the event at the detector (ISO 8601)",
    )

    is_pre_sn: bool = Field(
        default=False,
        description="True if associated with pre-supernova activity",
    )

    is_test: bool = Field(
        default=False,
        description="True if this is a test message",
    )

    is_firedrill: bool = Field(
        default=False,
        description="True if associated with a fire drill",
    )

    meta: Optional[dict] = Field(
        default=None,
        description="Attached metadata",
    )

    schema_version: str = Field(
        default=SCHEMA_VERSION,
        description="Schema version of the message",
    )

    detector_name: str = Field(
        ...,
        description="Name of the detector that sent the message",
    )

    @model_validator(mode="after")
    def _auto_generate_id(self):
        """Auto-generate human-readable ID if not provided."""
        if self.id is None:
            time_part = self.machine_time_utc or "unknown"
            tier_name = self.tier.value if hasattr(self.tier, 'value') else self.tier
            self.id = f"{self.detector_name}_{tier_name}_{time_part}"
        return self

    @field_validator("sent_time_utc", "machine_time_utc", mode="before")
    @classmethod
    def _ensure_string_timestamp(cls, v):
        """Convert datetime objects to ISO strings."""
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    def to_json(self) -> dict:
        """Serialize to JSON-compatible dict."""
        return self.model_dump(mode="json")


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

class HeartbeatMessage(SNEWS2MessageBase):
    """
    Heartbeat message — detector health monitoring.

    Sent periodically by each detector to indicate it is online.
    """
    tier: Tier = Field(default=Tier.HEARTBEAT)

    detector_status: DetectorStatus = Field(
        ...,
        description="Detector status: ON or OFF",
    )


# ---------------------------------------------------------------------------
# Retraction
# ---------------------------------------------------------------------------

class RetractionMessage(SNEWS2MessageBase):
    """
    Retraction message — withdraw a previous alert.

    Must specify either `retract_message_uuid` (retract specific message)
    or `retract_latest_n > 0` (retract the N most recent messages).
    """
    tier: Tier = Field(default=Tier.RETRACTION)

    retract_message_uuid: Optional[str] = Field(
        default=None,
        description="UUID of the message to retract",
    )

    retract_latest_n: int = Field(
        default=0,
        ge=0,
        description="Retract the N most recent messages from this detector",
    )

    retraction_reason: Optional[str] = Field(
        default=None,
        description="Reason for the retraction",
    )

    @model_validator(mode="after")
    def _validate_retraction_target(self):
        """Ensure exactly one retraction method is specified."""
        if self.retract_latest_n > 0 and self.retract_message_uuid is not None:
            raise ValueError(
                "Cannot specify both retract_message_uuid and retract_latest_n"
            )
        if self.retract_latest_n == 0 and self.retract_message_uuid is None:
            raise ValueError(
                "Must specify either retract_message_uuid or retract_latest_n > 0"
            )
        return self


# ---------------------------------------------------------------------------
# Tier base (Coincidence + Significance + Timing share p_val)
# ---------------------------------------------------------------------------

class TierMessageBase(SNEWS2MessageBase):
    """Base for science-tier messages that carry a p-value."""

    p_val: Optional[float] = Field(
        default=None,
        ge=0,
        le=1,
        description="p-value of coincidence",
    )


# ---------------------------------------------------------------------------
# Coincidence Tier
# ---------------------------------------------------------------------------

class CoincidenceTierMessage(TierMessageBase):
    """
    Coincidence tier message — multi-detector coincidence detection.

    Sent when a detector observes a neutrino burst that may be
    coincident with observations from other detectors.
    """
    tier: Tier = Field(default=Tier.COINCIDENCE_TIER)

    neutrino_time_utc: str = Field(
        ...,
        description="Time of the first neutrino (ISO 8601 with ns precision)",
    )

    @field_validator("neutrino_time_utc", mode="before")
    @classmethod
    def _ensure_neutrino_time_string(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v


# ---------------------------------------------------------------------------
# Significance Tier
# ---------------------------------------------------------------------------

class SignificanceTierMessage(TierMessageBase):
    """
    Significance tier message — statistical significance of neutrino burst.

    Reports p-values across multiple time bins to characterize the
    statistical confidence of a detected neutrino excess.
    """
    tier: Tier = Field(default=Tier.SIGNIFICANCE_TIER)

    p_values: List[float] = Field(
        ...,
        description="p-values for each time bin",
    )

    t_bin_width_sec: float = Field(
        ...,
        ge=0,
        description="Width of each time bin in seconds",
    )

    @field_validator("p_values")
    @classmethod
    def _validate_p_values(cls, v):
        if any(p < 0 or p > 1 for p in v):
            raise ValueError("All p-values must be between 0 and 1")
        if len(v) == 0:
            raise ValueError("p_values must contain at least one value")
        return v


# ---------------------------------------------------------------------------
# Timing Tier
# ---------------------------------------------------------------------------

class TimingTierMessage(TierMessageBase):
    """
    Timing tier message — precise neutrino arrival timing.

    Provides either individual neutrino timestamps (as offsets from
    start_time_utc in nanoseconds) or binned event counts.
    """
    tier: Tier = Field(default=Tier.TIMING_TIER)

    neutrino_time_utc: str = Field(
        ...,
        description="Time of the first neutrino (ISO 8601)",
    )

    start_time_utc: str = Field(
        ...,
        description="Base time for the timing series (ISO 8601)",
    )

    timing_series: List[int] = Field(
        ...,
        min_length=1,
        description=(
            "If time_bin_width_ns is set: binned event counts. "
            "Otherwise: individual time offsets from start_time_utc in ns."
        ),
    )

    time_bin_width_ns: Optional[int] = Field(
        default=None,
        gt=0,
        description="Bin width for histogrammed event counts (ns)",
    )

    background_rate_Hz: Optional[float] = Field(
        default=None,
        ge=0,
        description="Detector background rate in Hz",
    )

    detection_channel: Optional[DetectionChannel] = Field(
        default=None,
        description="Neutrino detection channel",
    )

    @field_validator("neutrino_time_utc", "start_time_utc", mode="before")
    @classmethod
    def _ensure_time_string(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    def is_binned(self) -> bool:
        """Return True if this is a binned time series."""
        return self.time_bin_width_ns is not None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

TIER_MODEL_MAP = {
    Tier.HEARTBEAT: HeartbeatMessage,
    Tier.RETRACTION: RetractionMessage,
    Tier.COINCIDENCE_TIER: CoincidenceTierMessage,
    Tier.SIGNIFICANCE_TIER: SignificanceTierMessage,
    Tier.TIMING_TIER: TimingTierMessage,
}


def parse_snews2_message(data: dict) -> SNEWS2MessageBase:
    """
    Parse a JSON dict into the appropriate SNEWS2 message model.

    Args:
        data: Dict with at least a 'tier' field identifying the message type.

    Returns:
        Validated Pydantic model instance.

    Raises:
        ValueError: If tier is missing or unknown.
    """
    tier_str = data.get("tier")
    if tier_str is None:
        raise ValueError("Message missing required 'tier' field")

    try:
        tier = Tier(tier_str)
    except ValueError:
        raise ValueError(f"Unknown tier: {tier_str}")

    model_cls = TIER_MODEL_MAP[tier]
    return model_cls(**data)
