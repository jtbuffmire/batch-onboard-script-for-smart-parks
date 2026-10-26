#!/usr/bin/env python3
"""ADR-0044 (app.buoy.fish docs/adr/0044-gps-profiles.md): the BLE writes are
the same settings as the app's LoRa profile frame. The golden vector is the
contract; BLE writes one setting at a time, so the TLVs compare as a set."""
import importlib.util
import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("BUOY_VENV_BOOTSTRAPPED", "1")
spec = importlib.util.spec_from_file_location(
    "batch_onboard", Path(__file__).with_name("batch-onboard-us915.py"))
bo = importlib.util.module_from_spec(spec)
sys.modules["batch_onboard"] = bo
spec.loader.exec_module(bo)

HOURLY_TLVS = ["2E0100", "290100", "0204100E0000", "2604100E0000", "0304100E0000", "0104100E0000"]


def tlvs(profile):
    return [bytes([sid, len(value)]).hex().upper() + value.hex().upper()
            for _name, sid, value in bo._profile_steps(profile)]


class ProfilesGoldenTest(unittest.TestCase):
    def test_hourly_is_the_only_profile(self):
        self.assertEqual([p["id"] for p in bo.UPLINK_PROFILES], ["hourly"])

    def test_hourly_writes_match_the_golden_vector(self):
        self.assertEqual(sorted(tlvs(bo.PROFILES_BY_ID["hourly"])), sorted(HOURLY_TLVS))

    def test_every_profile_turns_motion_triggered_gps_off(self):
        for profile in bo.UPLINK_PROFILES:
            self.assertIn("2E0100", tlvs(profile), profile["id"])


if __name__ == "__main__":
    unittest.main()
