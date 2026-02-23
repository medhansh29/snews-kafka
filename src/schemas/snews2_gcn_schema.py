"""
SNEWS 2.0 to GCN JSON Schema transformation.

Encapsulates the 5 SNEWS2 tiers into a single schema for GCN publication.
"""

from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, Field

from .snews2_messages import SNEWS2MessageBase, Tier


class SNEWS2GCNNotice(BaseModel):
    """
    Unified JSON schema representing a SNEWS 2.0 alert wrapped for GCN.
    """
    schema_version: str = Field(default="1.0", description="Schema version")
    gcn_notice_type: str = Field(default="SNEWS2_ALERT", description="GCN Notice Type")
    is_test: bool = Field(..., description="True if this is a test message")
    is_firedrill: bool = Field(..., description="True if associated with a fire drill")
    snews2_tier: Tier = Field(..., description="The tier of the original message")
    snews2_message_uuid: str = Field(..., description="UUID of the original message")
    detector_name: str = Field(..., description="Originating detector")
    event_time_utc: str = Field(..., description="Primary timestamp for the event")
    tier_data: Dict[str, Any] = Field(..., description="Tier-specific data payload")


def transform_snews2_to_gcn(msg: SNEWS2MessageBase) -> SNEWS2GCNNotice:
    """
    Transform a SNEWS2 message into a custom GCN-compatible payload.
    """
    # Exclude base fields so tier_data only holds tier-specific properties
    tier_data = msg.model_dump(exclude={
        "id", "uuid", "tier", "sent_time_utc", "machine_time_utc", 
        "is_pre_sn", "is_test", "is_firedrill", "meta", "schema_version", "detector_name"
    }, exclude_none=True)
    
    # Determine the most accurate event time
    from datetime import timezone
    event_time = msg.machine_time_utc or msg.sent_time_utc or datetime.now(timezone.utc).isoformat()
    if msg.tier in [Tier.COINCIDENCE_TIER, Tier.TIMING_TIER, "CoincidenceTier", "TimingTier"]:
        event_time = getattr(msg, "neutrino_time_utc", event_time)
        
    return SNEWS2GCNNotice(
        is_test=msg.is_test,
        is_firedrill=msg.is_firedrill,
        snews2_tier=msg.tier,
        snews2_message_uuid=msg.uuid,
        detector_name=msg.detector_name,
        event_time_utc=event_time,
        tier_data=tier_data
    )
