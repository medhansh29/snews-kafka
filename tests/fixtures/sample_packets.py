"""
Sample SNEWS Binary Packets for Testing

Mock packets based on documented GCN/SNEWS examples.
"""

import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.schemas.legacy_snews import LegacySNEWSParser, DetectorFlags


def create_real_coincidence_with_superk() -> bytes:
    """
    Real coincidence notice with Super-K participation (defined RA/Dec).
    
    Based on documented example:
    - Trigger 131
    - TJD 13501, SOD 78275.83 (2005-05-22T21:44:35.83Z)
    - RA 273.34°, Dec 44.60°
    - Super-K Good, LVD Good, IceCube Possible, Daya Bay Possible
    """
    detector_misc = (
        DetectorFlags.SUPER_K_PARTICIPATED | DetectorFlags.SUPER_K_GOOD |
        DetectorFlags.LVD_PARTICIPATED | DetectorFlags.LVD_GOOD |
        DetectorFlags.ICECUBE_PARTICIPATED | DetectorFlags.ICECUBE_POSSIBLE |
        DetectorFlags.DAYA_BAY_PARTICIPATED | DetectorFlags.DAYA_BAY_POSSIBLE
    )
    
    return LegacySNEWSParser.create_mock_packet(
        trigger_num=131,
        event_tjd=13501,
        event_sod=78275.83,
        event_ra=273.34,
        event_dec=44.60,
        event_fluence=18,
        event_error=10.0,
        event_cont=68.0,
        duration=10.0,
        is_test=False,
        is_coincidence=True,
        detector_misc=detector_misc,
    )


def create_real_coincidence_without_superk() -> bytes:
    """
    Real coincidence notice without Super-K (undefined RA/Dec).
    
    Based on documented example with undefined coordinates.
    """
    detector_misc = (
        DetectorFlags.LVD_PARTICIPATED | DetectorFlags.LVD_GOOD |
        DetectorFlags.ICECUBE_PARTICIPATED | DetectorFlags.ICECUBE_POSSIBLE |
        DetectorFlags.DAYA_BAY_PARTICIPATED | DetectorFlags.DAYA_BAY_POSSIBLE
    )
    
    return LegacySNEWSParser.create_mock_packet(
        trigger_num=132,
        event_tjd=13501,
        event_sod=78275.83,
        event_ra=None,
        event_dec=None,
        event_fluence=9,
        event_error=360.0,  # Full-sky error when no position
        event_cont=68.0,
        duration=10.0,
        is_test=False,
        is_coincidence=True,
        detector_misc=detector_misc,
    )


def create_test_coincidence() -> bytes:
    """
    Test coincidence notice (weekly scheduled test).
    
    Generated every Tuesday at 12:00 US Eastern.
    """
    detector_misc = (
        DetectorFlags.SUPER_K_PARTICIPATED | DetectorFlags.SUPER_K_GOOD |
        DetectorFlags.LVD_PARTICIPATED | DetectorFlags.LVD_GOOD |
        DetectorFlags.ICECUBE_PARTICIPATED | DetectorFlags.ICECUBE_POSSIBLE |
        DetectorFlags.KAMLAND_PARTICIPATED | DetectorFlags.KAMLAND_GOOD |
        DetectorFlags.BOREXINO_PARTICIPATED | DetectorFlags.BOREXINO_GOOD
    )
    
    return LegacySNEWSParser.create_mock_packet(
        trigger_num=1000132,
        event_tjd=13501,
        event_sod=43200.0,  # 12:00:00 UT
        event_ra=273.34,
        event_dec=44.60,
        event_fluence=18,
        event_error=2.0,
        event_cont=68.0,
        duration=39.81,
        is_test=True,
        is_coincidence=True,
        detector_misc=detector_misc,
    )


def create_individual_notice() -> bytes:
    """
    Individual detector notice (single detector high-confidence).
    """
    detector_misc = DetectorFlags.SUPER_K_PARTICIPATED | DetectorFlags.SUPER_K_OVERRIDE
    
    return LegacySNEWSParser.create_mock_packet(
        trigger_num=200,
        event_tjd=17023,  # 01 Jan 2015
        event_sod=36000.0,  # 10:00:00 UT
        event_ra=180.0,
        event_dec=-30.0,
        event_fluence=5,
        event_error=8.0,
        event_cont=90.0,
        duration=0.5,
        is_test=False,
        is_coincidence=False,
        detector_misc=detector_misc,
    )


# All sample packets
SAMPLE_PACKETS = {
    "real_coincidence_with_superk": create_real_coincidence_with_superk,
    "real_coincidence_without_superk": create_real_coincidence_without_superk,
    "test_coincidence": create_test_coincidence,
    "individual_notice": create_individual_notice,
}
