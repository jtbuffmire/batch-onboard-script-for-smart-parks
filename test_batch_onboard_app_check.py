#!/usr/bin/env python3
"""Tests for the app-registry stage of batch-onboard-us915.py (ADR-0025
stages 1, 3, 6). The script is imported as a module with the venv guard
bypassed; every HTTP call is injected, nothing reaches the network."""
import importlib.util
import os
import sys
import unittest
import urllib.error
from pathlib import Path

os.environ.setdefault("BUOY_VENV_BOOTSTRAPPED", "1")
spec = importlib.util.spec_from_file_location(
    "batch_onboard", Path(__file__).with_name("batch-onboard-us915.py"))
bo = importlib.util.module_from_spec(spec)
sys.modules["batch_onboard"] = bo
spec.loader.exec_module(bo)


def http_error(code):
    def fetch(url, timeout=15.0):
        raise urllib.error.HTTPError(url, code, "x", {}, None)
    return fetch


class AppRegistrationStateTest(unittest.TestCase):
    """GET /api/devices/lookup/<eui> is the registry's public answer: 200 is
    registered, 404 is not, and anything else is UNKNOWN — never conflated
    with "not registered", or an app blip becomes a fleet of false orphans."""

    def test_200_is_registered(self):
        state = bo.app_registration_state("0016C001F05120E5", "https://app.example",
                                          fetch=lambda url, timeout=15.0: {"device": {}})
        self.assertEqual(state, "registered")

    def test_404_is_unregistered(self):
        state = bo.app_registration_state("0016C001F05120D6", "https://app.example",
                                          fetch=http_error(404))
        self.assertEqual(state, "unregistered")

    def test_5xx_is_unknown_not_unregistered(self):
        state = bo.app_registration_state("0016C001F05120D6", "https://app.example",
                                          fetch=http_error(503))
        self.assertEqual(state, "unknown")

    def test_transport_failure_is_unknown(self):
        def boom(url, timeout=15.0):
            raise OSError("dns")
        state = bo.app_registration_state("0016C001F05120D6", "https://app.example", fetch=boom)
        self.assertEqual(state, "unknown")

    def test_lookup_hits_the_public_endpoint_for_that_eui(self):
        seen = []
        bo.app_registration_state("0016c001f05120d6", "https://app.example/",
                                  fetch=lambda url, timeout=15.0: seen.append(url) or {})
        self.assertEqual(seen, ["https://app.example/api/devices/lookup/0016C001F05120D6"])


class JoinedButNotInAppReportTest(unittest.TestCase):
    """Two-of-three is a failure to surface, not a success (ADR-0025 D1):
    a device the LNS has joined but the app has never heard of gets its own
    section in the post-flight report, with the SP# a human can find on the
    label and the name ChirpStack shows."""

    def test_joined_and_unregistered_devices_are_listed(self):
        states = {"0016C001F05120D6": "unregistered", "0016C001F05120E5": "registered"}
        activations = {"0016C001F05120D6": "78000191", "0016C001F05120E5": "78000195"}
        sp = {"0016C001F05120D6": "SP100204", "0016C001F05120E5": "SP100197"}
        names = {"0016C001F05120D6": "Chain Rolling Seahorse (20D6)"}

        rows = bo.joined_but_not_in_app(states, activations, sp, names)

        self.assertEqual(rows, [("SP100204", "0016C001F05120D6", "Chain Rolling Seahorse (20D6)")])

    def test_unjoined_devices_are_not_listed_even_if_unregistered(self):
        rows = bo.joined_but_not_in_app({"A" * 16: "unregistered"}, {"A" * 16: None}, {}, {})
        self.assertEqual(rows, [])

    def test_unknown_is_reported_separately_not_as_orphan(self):
        rows = bo.joined_but_not_in_app({"A" * 16: "unknown"}, {"A" * 16: "78000191"}, {}, {})
        self.assertEqual(rows, [])

    def test_section_text_names_the_registry_gap_and_how_to_close_it(self):
        text = bo.format_app_registry_section(
            [("SP100204", "0016C001F05120D6", "Chain Rolling Seahorse (20D6)")],
            unknown=["0016C001F05120E1"],
        )
        self.assertIn("JOINED BUT NOT IN APP (1)", text)
        self.assertIn("SP100204", text)
        self.assertIn("0016C001F05120D6", text)
        self.assertIn("--register-app --org", text)
        self.assertIn("could not be checked (1)", text)
        self.assertIn("0016C001F05120E1", text)

    def test_section_is_empty_when_all_three_registries_agree(self):
        self.assertEqual(bo.format_app_registry_section([], unknown=[]), "")


class RegisterInAppTest(unittest.TestCase):
    """Stage 3: registration is explicit, machine-shaped, and goes through the
    app's internal endpoint (ADR-0025 D4). The org is whatever the operator
    named -- the tool never infers one (D2)."""

    def test_posts_the_device_to_the_internal_endpoint_with_the_shared_token(self):
        seen = {}

        def post(url, body, headers, timeout=15.0):
            seen.update(url=url, body=body, headers=headers)
            return 201, {"data": {"device_id": "0016c001-f051-20d6-0016-c001f05120d6",
                                  "last_ping": "2026-09-10T20:05:53Z"}}

        result = bo.register_device_in_app(
            "0016c001f05120d6", "Chain Rolling Seahorse (20D6)", "Buoy Fish Inventory",
            app_url="https://app.example/", token="tok", post=post)

        self.assertEqual(seen["url"], "https://app.example/api/internal/devices/register")
        self.assertEqual(seen["headers"]["X-Mappers-Admin-Token"], "tok")
        self.assertEqual(seen["body"], {"dev_eui": "0016C001F05120D6",
                                        "name": "Chain Rolling Seahorse (20D6)",
                                        "device_type": "buoy",
                                        "organization_name": "Buoy Fish Inventory"})
        self.assertEqual(result, ("registered", "0016c001-f051-20d6-0016-c001f05120d6",
                                  "2026-09-10T20:05:53Z"))

    def test_numeric_org_is_sent_as_an_id(self):
        seen = {}
        bo.register_device_in_app("A" * 16, "n", "42", app_url="u", token="t",
                                  post=lambda url, body, headers, timeout=15.0: seen.update(body=body) or (201, {"data": {}}))
        self.assertEqual(seen["body"]["organization_id"], 42)
        self.assertNotIn("organization_name", seen["body"])

    def test_already_registered_is_reported_not_retried(self):
        result = bo.register_device_in_app("A" * 16, "n", "org", app_url="u", token="t",
                                           post=lambda *a, **k: (409, {"error": "already_registered", "data": {"device_id": "x"}}))
        self.assertEqual(result[0], "already_registered")

    def test_other_failures_are_reported_with_the_body(self):
        result = bo.register_device_in_app("A" * 16, "n", "org", app_url="u", token="t",
                                           post=lambda *a, **k: (404, {"error": "organization_not_found"}))
        self.assertEqual(result[0], "failed")
        self.assertIn("organization_not_found", result[1])

    def test_never_sends_a_device_id(self):
        seen = {}
        bo.register_device_in_app("A" * 16, "n", "org", app_url="u", token="t",
                                  post=lambda url, body, headers, timeout=15.0: seen.update(body=body) or (201, {"data": {}}))
        self.assertNotIn("device_id", seen["body"])


class LedgerAppRegistrationTest(unittest.TestCase):
    """Stage 6: the ledger records what this host did about the app registry,
    and stays non-authoritative."""

    def test_mark_app_registered_persists_flag_and_time(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "onboarded.json"
            ledger = bo.Ledger(path)
            ledger.record("A" * 16, {"status": "ok"})
            ledger.mark_app_registered("A" * 16, True, at="2026-09-10T22:00:00+00:00")

            again = bo.Ledger(path)
            entry = again.data["devices"]["A" * 16]
            self.assertEqual(entry["status"], "ok")
            self.assertTrue(entry["app_registered"])
            self.assertEqual(entry["app_registered_at"], "2026-09-10T22:00:00+00:00")

    def test_marking_unregistered_records_false_without_a_time(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            ledger = bo.Ledger(Path(d) / "onboarded.json")
            ledger.record("A" * 16, {"status": "ok"})
            ledger.mark_app_registered("A" * 16, False)
            entry = ledger.data["devices"]["A" * 16]
            self.assertFalse(entry["app_registered"])
            self.assertNotIn("app_registered_at", entry)


if __name__ == "__main__":
    unittest.main()
