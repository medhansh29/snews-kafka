"""
Unit tests for SNEWS Transformer

Tests binary parsing, TJD/SOD conversion, and JSON transformation.
"""

import pytest
from datetime import datetime, timezone

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.schemas.legacy_snews import (
    LegacySNEWSParser,
    LegacySNEWSPacket,
    TriggerIdFlags,
    DetectorFlags,
)
from src.schemas.gcn_unified import SNEWSNotice, NoticeType, NoticeSubtype
from src.transformer import SNEWSTransformer, tjd_sod_to_datetime


class TestTJDConversion:
    """Tests for TJD/SOD to datetime conversion."""
    
    def test_documented_example(self):
        """Test conversion of documented example: TJD 13501, SOD 78275.83"""
        result = tjd_sod_to_datetime(13501, 78275.83)
        
        # TJD 13501 = 2005-05-11 (based on TJD epoch 1968-05-24)
        # Note: GCN example shows "05/05/22" but this conflicts with DOY 140
        # Using authoritative TJD definition: TJD = JD - 2440000.5
        assert result.year == 2005
        assert result.month == 5
        assert result.day == 11
        assert result.hour == 21
        assert result.minute == 44
        assert result.second == 35
        assert result.tzinfo == timezone.utc
    
    def test_midnight(self):
        """Test conversion at midnight (SOD = 0)."""
        result = tjd_sod_to_datetime(17023, 0.0)
        
        # TJD 17023 should be 2015-01-01
        assert result.year == 2015
        assert result.month == 1
        assert result.day == 1
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0
    
    def test_end_of_day(self):
        """Test conversion at end of day."""
        result = tjd_sod_to_datetime(17023, 86399.99)
        
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59


class TestBinaryParser:
    """Tests for legacy binary packet parsing."""
    
    def test_parse_valid_packet(self):
        """Test parsing a valid 160-byte packet."""
        data = LegacySNEWSParser.create_mock_packet(
            trigger_num=131,
            event_tjd=13501,
            event_sod=78275.83,
            event_ra=273.34,
            event_dec=44.60,
        )
        
        packet = LegacySNEWSParser.parse(data)
        
        assert packet.trigger_num == 131
        assert packet.event_tjd == 13501
        assert abs(packet.event_sod - 78275.83) < 0.01
        assert abs(packet.event_ra - 273.34) < 0.001
        assert abs(packet.event_dec - 44.60) < 0.001
    
    def test_parse_undefined_radec(self):
        """Test parsing packet with undefined RA/Dec."""
        data = LegacySNEWSParser.create_mock_packet(
            trigger_num=132,
            event_ra=None,
            event_dec=None,
        )
        
        packet = LegacySNEWSParser.parse(data)
        
        assert packet.event_ra is None
        assert packet.event_dec is None
        assert packet.radec_undefined is True
    
    def test_parse_test_notice(self):
        """Test parsing test notice flag."""
        data = LegacySNEWSParser.create_mock_packet(is_test=True)
        packet = LegacySNEWSParser.parse(data)
        
        assert packet.is_test is True
    
    def test_parse_coincidence_flag(self):
        """Test parsing coincidence vs individual flag."""
        coincidence_data = LegacySNEWSParser.create_mock_packet(is_coincidence=True)
        individual_data = LegacySNEWSParser.create_mock_packet(is_coincidence=False)
        
        coincidence = LegacySNEWSParser.parse(coincidence_data)
        individual = LegacySNEWSParser.parse(individual_data)
        
        assert coincidence.is_coincidence is True
        assert individual.is_coincidence is False
    
    def test_invalid_packet_size(self):
        """Test rejection of invalid packet size."""
        with pytest.raises(ValueError, match="Expected 160 bytes"):
            LegacySNEWSParser.parse(b"too short")
    
    def test_invalid_packet_type(self):
        """Test rejection of wrong packet type."""
        # Create valid-sized packet but wrong type
        import struct
        longs = [100] + [0] * 39  # Type 100 instead of 149
        data = struct.pack(">40I", *longs)
        
        with pytest.raises(ValueError, match="Expected packet type 149"):
            LegacySNEWSParser.parse(data)


class TestDetectorStatus:
    """Tests for detector participation flag parsing."""
    
    def test_superk_good(self):
        """Test Super-K with good quality."""
        misc = DetectorFlags.SUPER_K_PARTICIPATED | DetectorFlags.SUPER_K_GOOD
        data = LegacySNEWSParser.create_mock_packet(detector_misc=misc)
        packet = LegacySNEWSParser.parse(data)
        
        status = packet.get_detector_status("super_k")
        assert status["participated"] is True
        assert status["good"] is True
        assert status["possible"] is False
    
    def test_multiple_detectors(self):
        """Test multiple detector participation."""
        misc = (
            DetectorFlags.SUPER_K_PARTICIPATED | DetectorFlags.SUPER_K_GOOD |
            DetectorFlags.LVD_PARTICIPATED | DetectorFlags.LVD_POSSIBLE |
            DetectorFlags.ICECUBE_PARTICIPATED
        )
        data = LegacySNEWSParser.create_mock_packet(detector_misc=misc)
        packet = LegacySNEWSParser.parse(data)
        
        statuses = packet.get_all_detector_statuses()
        
        assert statuses["super_k"]["participated"] is True
        assert statuses["super_k"]["good"] is True
        assert statuses["lvd"]["participated"] is True
        assert statuses["lvd"]["possible"] is True
        assert statuses["icecube"]["participated"] is True
        assert statuses["kamland"]["participated"] is False


class TestTransformer:
    """Tests for full transformation pipeline."""
    
    def test_full_transformation(self):
        """Test complete binary-to-JSON transformation."""
        data = LegacySNEWSParser.create_mock_packet(
            trigger_num=131,
            event_tjd=13501,
            event_sod=78275.83,
            event_ra=273.34,
            event_dec=44.60,
            event_fluence=18,
            is_test=False,
            is_coincidence=True,
        )
        
        notice = SNEWSTransformer.from_bytes(data)
        
        assert isinstance(notice, SNEWSNotice)
        assert notice.trigger_num == 131
        assert notice.fluence == 18
        assert notice.notice_type == NoticeType.REAL
        assert notice.subtype == NoticeSubtype.COINCIDENCE
        assert notice.coordinates.ra == pytest.approx(273.34, abs=0.01)
    
    def test_json_serialization(self):
        """Test JSON output is valid."""
        data = LegacySNEWSParser.create_mock_packet()
        notice = SNEWSTransformer.from_bytes(data)
        json_str = SNEWSTransformer.to_json(notice)
        
        assert isinstance(json_str, str)
        assert "trigger_num" in json_str
        assert "event_time" in json_str
        assert "coordinates" in json_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
