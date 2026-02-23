import pytest
from datetime import datetime

from src.schemas.snews2_messages import CoincidenceTierMessage, Tier, DetectorStatus, DetectionChannel, HeartbeatMessage, TimingTierMessage, RetractionMessage, SignificanceTierMessage
from src.schemas.snews2_gcn_schema import transform_snews2_to_gcn, SNEWS2GCNNotice

def test_transform_coincidence_tier_to_gcn():
    msg = CoincidenceTierMessage(
        detector_name="Super-K",
        neutrino_time_utc="2025-01-15T14:30:00.123456+00:00",
        p_val=0.07,
        is_test=True,
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    
    assert isinstance(gcn_notice, SNEWS2GCNNotice)
    assert gcn_notice.is_test is True
    assert gcn_notice.snews2_tier == Tier.COINCIDENCE_TIER
    assert gcn_notice.snews2_message_uuid == msg.uuid
    assert gcn_notice.detector_name == "Super-K"
    assert gcn_notice.event_time_utc == "2025-01-15T14:30:00.123456+00:00"
    
    assert "neutrino_time_utc" in gcn_notice.tier_data
    assert gcn_notice.tier_data["p_val"] == 0.07

def test_transform_heartbeat_to_gcn():
    msg = HeartbeatMessage(
        detector_name="IceCube",
        detector_status=DetectorStatus.ON,
        machine_time_utc="2025-01-15T14:35:00.000000+00:00"
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    
    assert gcn_notice.snews2_tier == Tier.HEARTBEAT
    assert gcn_notice.detector_name == "IceCube"
    assert gcn_notice.event_time_utc == "2025-01-15T14:35:00.000000+00:00"
    assert gcn_notice.tier_data["detector_status"] == DetectorStatus.ON.value
    assert "machine_time_utc" not in gcn_notice.tier_data # excluded base field

def test_transform_timing_tier_to_gcn():
    msg = TimingTierMessage(
        detector_name="Borexino",
        neutrino_time_utc="2025-01-15T14:30:00.123456+00:00",
        start_time_utc="2025-01-15T14:30:00.000000+00:00",
        timing_series=[1000, 2000, 3000],
        detection_channel=DetectionChannel.NU_E
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    
    assert gcn_notice.snews2_tier == Tier.TIMING_TIER
    assert gcn_notice.event_time_utc == "2025-01-15T14:30:00.123456+00:00"
    assert gcn_notice.tier_data["start_time_utc"] == "2025-01-15T14:30:00.000000+00:00"
    assert gcn_notice.tier_data["timing_series"] == [1000, 2000, 3000]
    assert gcn_notice.tier_data["detection_channel"] == DetectionChannel.NU_E.value
    
def test_transform_retraction_to_gcn():
    msg = RetractionMessage(
        detector_name="Super-K",
        retract_latest_n=1,
        retraction_reason="False alarm",
        machine_time_utc="2025-01-15T14:40:00.000000+00:00"
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    assert gcn_notice.tier_data["retract_latest_n"] == 1
    assert gcn_notice.tier_data["retraction_reason"] == "False alarm"
    
def test_transform_significance_to_gcn():
    msg = SignificanceTierMessage(
        detector_name="HALO",
        p_values=[0.1, 0.05, 0.01],
        t_bin_width_sec=0.5,
        machine_time_utc="2025-01-15T14:30:00.000000+00:00"
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    assert gcn_notice.event_time_utc == "2025-01-15T14:30:00.000000+00:00"
    assert gcn_notice.tier_data["p_values"] == [0.1, 0.05, 0.01]
    assert gcn_notice.tier_data["t_bin_width_sec"] == 0.5
