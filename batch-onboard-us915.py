#!/usr/bin/env python3
"""
Batch onboard IRNAS Smart Parks GPS trackers over BLE.

Walks every IRNAS device in BLE range, optionally writes the LoRaWAN region
and a "quick profile" of GPS/uplink intervals, then ALWAYS issues cmd_reset
(0xA1) so the radio re-initializes from flash and the device emits a real,
network-visible JoinRequest. After all devices are visited it (optionally)
waits N minutes and asks ChirpStack which devices have actually joined.

Why always reboot?
------------------
The IRNAS firmware tracks LoRaWAN session state in RAM. After a fresh
factory boot the device may *report* lr_joined=true even though the
network has never seen a JoinRequest from it. cmd_join (0xA0) is silently
dropped in that state. Only cmd_reset (0xA1) wipes the stale RAM session
and forces a clean OTAA handshake on the persisted lr_region.

The only authoritative source of truth for "joined" is the network server
(ChirpStack lastSeenAt) -- never the device's self-report.

Usage
-----
Bare invocation runs the interactive wizard:

    ./batch-onboard.sh                         # the recommended entry point
    python3 batch-onboard-us915.py             # equivalent if your venv is active

Non-interactive flags skip the wizard and behave like the legacy script:

    python3 batch-onboard-us915.py --list
    python3 batch-onboard-us915.py --sp SP100280
    python3 batch-onboard-us915.py --sp SP100207 SP100208 SP100209
    python3 batch-onboard-us915.py --dry-run --sp SP100280
    python3 batch-onboard-us915.py --by-address 22EBCF88-3C86-72DA-DE0B-48BD2443E654 --force
    python3 batch-onboard-us915.py --scan-seconds 20 --force --region 3 --profile transit

LNS verification (post-flight)
------------------------------
After the BLE loop, the script waits --verify-wait-min minutes (default 10)
then queries ChirpStack once for the application's full device list and
reports JOINED / NOT JOINED for each device touched in this run. Joined
status is determined by lastSeenAt > the per-device cmd_reset timestamp
recorded in the ledger.

Credentials are looked up in this order (first match wins):
  1. CLI flags (--lns-url, --lns-api-key, --lns-app-id)
  2. ./.env next to this script (or --lns-env PATH)
  3. Process environment (CHIRPSTACK_BASE_URL etc.)
  4. Interactive wizard prompts (with optional save to ./.env)
Use --no-verify to skip post-flight verification entirely.

Identifiers & platform notes
----------------------------
- SP# is the advertised BLE local name (e.g. "SP100280"). It is the only
  identifier visible pre-connect that matches the physical label on the unit.
- DevEUI is readable only over GATT after connect; use it for validation.
- MAC address filtering is NOT supported on macOS: CoreBluetooth hides real
  BT addresses and hands bleak an opaque per-host UUID instead. Use --sp.

First run on macOS will prompt for Bluetooth permission -- grant it in
System Settings > Privacy & Security > Bluetooth, then re-run.

Ledger: ./onboarded.json next to this script.
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from bleak import BleakClient, BleakScanner
    from bleak.backends.device import BLEDevice
    from bleak.backends.scanner import AdvertisementData
    from bleak.exc import BleakError
except ImportError:
    sys.stderr.write(
        "bleak is required. Install with: python3 -m pip install -r requirements.txt\n"
    )
    sys.exit(1)

# questionary is only needed for the interactive wizard. Don't fail import if
# it's missing -- non-interactive flag invocations should still work without it.
try:
    import questionary
except ImportError:
    questionary = None  # type: ignore

# ---------------------------------------------------------------------------
# BLE protocol constants (from prod/configure-buoy-fish/index.html and settings JSON)
# ---------------------------------------------------------------------------

UART_SERVICE_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
UART_RX_CHAR_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"  # host -> device (write)
UART_TX_CHAR_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"  # device -> host (notify)

IRNAS_COMPANY_ID = 0x0A61

PORT_SETTINGS = 0x03
PORT_COMMANDS = 0x20

CMD_GET_SETTING = 0xA8
CMD_SEND_ALL_SETTINGS = 0xA7  # device dumps every setting it knows
CMD_JOIN = 0xA0   # trigger LoRa rejoin (re-init stack with current config)
CMD_RESET = 0xA1  # full device reboot -- the only reliable way to force a real JR
CMD_CONFIRM_PORT = 0x1F       # device replies on this port to confirm a command

SETTING_LR_REGION = 0x0F
SETTING_DEVICE_EUI = 0x11
SETTING_INIT_TIME = 0x07
SETTING_GPS_INIT_LON = 0x05
SETTING_GPS_INIT_LAT = 0x06
COORD_SCALE = 10_000_000  # IRNAS firmware stores lat/lon as int32 * 1e7

# Semtech modem region enum (smtc_modem_region_t). NOT the LoRaMac-node enum.
# Source of truth: .cursor/rules/irnas-lorawan-regions.mdc
REGIONS: dict[int, str] = {
    1:  "EU868",
    2:  "AS923_GRP1",
    3:  "US915",
    4:  "AU915",
    5:  "CN470",
    6:  "WW2G4",
    7:  "AS923_GRP2",
    8:  "AS923_GRP3",
    9:  "IN865",
    10: "KR920",
    11: "RU864",
    12: "CN470_RP1",
    13: "AS923_GRP4",
}
COMMON_REGIONS = [3, 1, 2, 4]  # US915, EU868, AS923, AU915

DEFAULT_SCAN_WINDOW_SEC = 10.0  # per scan cycle; override with --scan-seconds
PER_DEVICE_TIMEOUT_SEC = 45.0   # hard cap per device
NOTIFY_WAIT_SEC = 2.5           # how long to wait for a notification reply
INTER_DEVICE_PAUSE_SEC = 2.0    # CoreBluetooth grace between disconnect and next connect
BY_ADDRESS_LOOKUP_SEC = 10.0    # how long to scan for a specific address in --by-address mode
INTER_SETTING_WRITE_SEC = 0.25  # pacing between profile setting writes (matches web app)
DEFAULT_VERIFY_WAIT_MIN = 10    # minutes to wait before checking ChirpStack

SCRIPT_DIR = Path(__file__).resolve().parent
LEDGER_PATH = SCRIPT_DIR / "onboarded.json"
LOCAL_ENV_PATH = SCRIPT_DIR / ".env"  # default location for ChirpStack credentials
LAST_BATCH_PATH = SCRIPT_DIR / ".last-batch.json"  # remembers wizard answers


# ---------------------------------------------------------------------------
# Quick Profiles (mirror of UPLINK_PROFILES in prod/configure-buoy-fish/index.html)
# ---------------------------------------------------------------------------
#
# Source of truth: prod/configure-buoy-fish/index.html lines 179-222.
# Each profile is a sequence of setting writes applied via the same per-setting
# write frame the web app uses. Setting IDs are pulled from
# prod/configure-buoy-fish/settings/settings_v7.0.0.json at runtime would be
# nice, but we hardcode here for offline use; the IDs have been stable across
# v6.x and v7.x so the risk is low. Cross-check if a new firmware ships.
#
# Each profile entry maps a setting NAME (used in logs) to (setting_id, value, length).
# Length is in bytes; values are encoded little-endian (uint8/uint16/uint32) or
# bool (1 byte 0/1) by _encode_setting() below.

# Setting IDs (hex from settings_v7.0.0.json)
SID_LR_GPS_INTERVAL          = 0x01
SID_UBLOX_SEND_INTERVAL      = 0x02
SID_STATUS_SEND_INTERVAL     = 0x03
SID_UBLOX_SEND_INTERVAL_2    = 0x25
SID_UBLOX_INTERVAL1_START    = 0x27
SID_UBLOX_INTERVAL2_START    = 0x52
SID_UBLOX_MULTIPLE_INTERVALS = 0x29
SID_ENABLE_MOTION_TRIG_GPS   = 0x2E
SID_MOTION_THS               = 0x2D


def _u32(v: int) -> bytes:
    return int(v).to_bytes(4, "little", signed=False)


def _i32(v: int) -> bytes:
    return int(v).to_bytes(4, "little", signed=True)


def _u8(v: int) -> bytes:
    return int(v).to_bytes(1, "little", signed=False)


def _bool(v: bool) -> bytes:
    return b"\x01" if v else b"\x00"


def coord_to_int(degrees: float) -> int:
    """Convert decimal degrees to the int32 representation IRNAS uses (deg * 1e7)."""
    return int(round(degrees * COORD_SCALE))


def int_to_coord(scaled: int) -> float:
    """Inverse of coord_to_int; for display."""
    return scaled / COORD_SCALE


def _profile_steps(profile: dict) -> list[tuple[str, int, bytes]]:
    """Return a list of (display_name, setting_id, value_bytes) for a profile."""
    steps: list[tuple[str, int, bytes]] = [
        ("lr_gps_interval",      SID_LR_GPS_INTERVAL,       _u32(profile["lr_gps"])),
        ("ublox_send_interval",  SID_UBLOX_SEND_INTERVAL,   _u32(profile["gps_day"])),
        ("ublox_send_interval_2", SID_UBLOX_SEND_INTERVAL_2, _u32(profile["gps_night"])),
        ("status_send_interval", SID_STATUS_SEND_INTERVAL,  _u32(profile["status"])),
        ("ublox_multiple_intervals", SID_UBLOX_MULTIPLE_INTERVALS,
         _bool(profile["gps_day"] != profile["gps_night"])),
    ]
    if profile.get("day_start") is not None:
        steps.append(("ublox_interval1_start", SID_UBLOX_INTERVAL1_START, _u8(profile["day_start"])))
    if profile.get("night_start") is not None:
        steps.append(("ublox_interval2_start", SID_UBLOX_INTERVAL2_START, _u8(profile["night_start"])))
    steps.append(("enable_motion_trig_gps", SID_ENABLE_MOTION_TRIG_GPS,
                  _bool(profile["motion_trig_gps"])))
    if profile.get("motion_ths") is not None:
        steps.append(("motion_ths", SID_MOTION_THS, _u8(profile["motion_ths"])))
    return steps


UPLINK_PROFILES: list[dict] = [
    {
        "id": "transit", "name": "Transit",
        "description": "GPS once daily, status 3x daily. Shipping & storage.",
        "lr_gps": 86400, "gps_day": 86400, "gps_night": 86400, "status": 28800,
        "day_start": None, "night_start": None, "battery_days": 365,
        "motion_trig_gps": False, "motion_ths": None,
    },
    {
        "id": "daily", "name": "Daily",
        "description": "GPS every 12 hours. Minimum usage for basic tracking.",
        "lr_gps": 43200, "gps_day": 43200, "gps_night": 43200, "status": 43200,
        "day_start": None, "night_start": None, "battery_days": 365,
        "motion_trig_gps": False, "motion_ths": None,
    },
    {
        "id": "battery_saver", "name": "Battery Saver",
        "description": "GPS every 2h day / 4h night. Motion-triggered. Long deployments.",
        "lr_gps": 7200, "gps_day": 7200, "gps_night": 14400, "status": 3600,
        "day_start": 4, "night_start": 19, "battery_days": 180,
        "motion_trig_gps": True, "motion_ths": None,
    },
    {
        "id": "hourly", "name": "Hourly",
        "description": "GPS every hour, motion-triggered.",
        "lr_gps": 3600, "gps_day": 3600, "gps_night": 3600, "status": 3600,
        "day_start": None, "night_start": None, "battery_days": 90,
        "motion_trig_gps": True, "motion_ths": None,
    },
    {
        "id": "frequent", "name": "Frequent",
        "description": "GPS every 15 min. Active tracking, higher battery use.",
        "lr_gps": 900, "gps_day": 900, "gps_night": 900, "status": 1800,
        "day_start": None, "night_start": None, "battery_days": 14,
        "motion_trig_gps": False, "motion_ths": None,
    },
    {
        "id": "test", "name": "Test",
        "description": "GPS & status every 15 min. Shake to trigger. Pre-deployment verification.",
        "lr_gps": 900, "gps_day": 900, "gps_night": 900, "status": 900,
        "day_start": None, "night_start": None, "battery_days": 14,
        "motion_trig_gps": True, "motion_ths": 2,
    },
]
PROFILES_BY_ID: dict[str, dict] = {p["id"]: p for p in UPLINK_PROFILES}


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------

class Ledger:
    """Persistent record of devices we've already processed, keyed by DevEUI."""

    def __init__(self, path: Path):
        self.path = path
        self.data: dict = {"version": 1, "devices": {}}
        if path.exists():
            try:
                self.data = json.loads(path.read_text())
                if "devices" not in self.data:
                    self.data["devices"] = {}
            except (json.JSONDecodeError, OSError) as e:
                print(f"[WARN] Could not read ledger at {path}: {e}. Starting fresh.")

    def already_done(self, dev_eui: str) -> bool:
        entry = self.data["devices"].get(dev_eui)
        return bool(entry and entry.get("status") == "ok")

    def get_reset_at(self, dev_eui: str) -> Optional[str]:
        entry = self.data["devices"].get(dev_eui)
        return entry.get("reset_at") if entry else None

    def record(self, dev_eui: str, entry: dict) -> None:
        self.data["devices"][dev_eui] = entry
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True))
        tmp.replace(self.path)

    def count_ok(self) -> int:
        return sum(1 for e in self.data["devices"].values() if e.get("status") == "ok")


# ---------------------------------------------------------------------------
# BLE protocol helpers
# ---------------------------------------------------------------------------

def frame_set_setting(setting_id: int, value_bytes: bytes) -> bytes:
    """Build a settings-write frame: [PORT_SETTINGS, id, length, ...value]."""
    return bytes([PORT_SETTINGS, setting_id, len(value_bytes)]) + value_bytes


def frame_get_setting(setting_id: int) -> bytes:
    """Build a 'get specific setting' command frame."""
    return bytes([PORT_COMMANDS, CMD_GET_SETTING, 0x01, setting_id])


def parse_settings_response(data: bytes) -> dict[int, bytes]:
    """Parse a port-3 notification payload into {setting_id: value_bytes}."""
    if not data or data[0] != PORT_SETTINGS:
        return {}
    out: dict[int, bytes] = {}
    i = 1
    while i + 1 < len(data):
        sid = data[i]
        slen = data[i + 1]
        start = i + 2
        end = start + slen
        if end > len(data):
            break
        out[sid] = bytes(data[start:end])
        i = end
    return out


def dev_eui_to_hex(value_bytes: bytes) -> str:
    """DevEUI is an 8-byte identifier sent in display order on the wire."""
    return value_bytes.hex().upper()


# ---------------------------------------------------------------------------
# Per-device session
# ---------------------------------------------------------------------------

class DeviceSession:
    def __init__(self, device: BLEDevice):
        self.device = device
        self.client: Optional[BleakClient] = None
        self.rx_buffer: list[bytes] = []
        self._settings_event = asyncio.Event()

    def _on_notify(self, _sender, data: bytearray) -> None:
        frame = bytes(data)
        self.rx_buffer.append(frame)
        if frame and frame[0] == PORT_SETTINGS:
            self._settings_event.set()

    async def __aenter__(self) -> "DeviceSession":
        self.client = BleakClient(self.device, timeout=15.0)
        await self.client.connect()
        await self.client.start_notify(UART_TX_CHAR_UUID, self._on_notify)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if self.client and self.client.is_connected:
                try:
                    await self.client.stop_notify(UART_TX_CHAR_UUID)
                except Exception:
                    pass
                await self.client.disconnect()
        except Exception:
            pass

    async def _write(self, frame: bytes) -> None:
        assert self.client is not None
        await self.client.write_gatt_char(UART_RX_CHAR_UUID, frame, response=True)

    async def _request_setting(self, setting_id: int, timeout: float = NOTIFY_WAIT_SEC) -> Optional[bytes]:
        self._settings_event.clear()
        pre_len = len(self.rx_buffer)
        await self._write(frame_get_setting(setting_id))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                await asyncio.wait_for(self._settings_event.wait(), timeout=deadline - time.monotonic())
            except asyncio.TimeoutError:
                break
            self._settings_event.clear()
            for frame in self.rx_buffer[pre_len:]:
                parsed = parse_settings_response(frame)
                if setting_id in parsed:
                    return parsed[setting_id]
        return None

    async def read_dev_eui(self) -> Optional[str]:
        raw = await self._request_setting(SETTING_DEVICE_EUI, timeout=NOTIFY_WAIT_SEC * 2)
        if raw and len(raw) == 8:
            return dev_eui_to_hex(raw)
        return None

    async def read_region(self) -> Optional[int]:
        raw = await self._request_setting(SETTING_LR_REGION)
        if raw and len(raw) == 1:
            return raw[0]
        return None

    async def write_region(self, region: int) -> None:
        await self._write(frame_set_setting(SETTING_LR_REGION, _u8(region)))
        await asyncio.sleep(0.4)  # let device persist before any read-back

    async def write_setting_if_different(
        self, name: str, setting_id: int, target: bytes
    ) -> bool:
        """Read current value; write only if it differs. Returns True iff write happened."""
        current = await self._request_setting(setting_id)
        if current == target:
            return False
        await self._write(frame_set_setting(setting_id, target))
        await asyncio.sleep(INTER_SETTING_WRITE_SEC)
        return True

    async def write_init_time(self, when: Optional[int] = None) -> int:
        """
        Write the device's init_time to `when` (Unix epoch seconds, defaults to now).
        Always writes -- the device's clock should match host time, even by 1s.
        Returns the timestamp written.
        """
        ts = int(when if when is not None else time.time())
        await self._write(frame_set_setting(SETTING_INIT_TIME, _u32(ts)))
        await asyncio.sleep(INTER_SETTING_WRITE_SEC)
        return ts

    async def write_gps_home(self, lat_deg: float, lon_deg: float) -> tuple[bool, bool]:
        """
        Write gps_init_lat / gps_init_lon if either differs from the target.
        Returns (lat_written, lon_written).
        """
        lat_bytes = _i32(coord_to_int(lat_deg))
        lon_bytes = _i32(coord_to_int(lon_deg))
        lat_written = await self.write_setting_if_different(
            "gps_init_lat", SETTING_GPS_INIT_LAT, lat_bytes
        )
        lon_written = await self.write_setting_if_different(
            "gps_init_lon", SETTING_GPS_INIT_LON, lon_bytes
        )
        return lat_written, lon_written

    async def read_all_settings(self, timeout: float = 5.0) -> dict[int, bytes]:
        """
        Send cmd_send_all_settings (0xA7) and parse the bulk dump into {id: bytes}.
        The device replies with several PORT_SETTINGS notifications followed by a
        confirmation frame on the command-confirm port.
        """
        pre_len = len(self.rx_buffer)
        await self._write(bytes([PORT_COMMANDS, CMD_SEND_ALL_SETTINGS, 0x00]))

        deadline = time.monotonic() + timeout
        confirmed = False
        while time.monotonic() < deadline and not confirmed:
            await asyncio.sleep(0.15)
            for frame in self.rx_buffer[pre_len:]:
                # Confirmation frame format: [0x1F, 0xF3, 0x02, 0xA7, 0x01]
                if (len(frame) >= 5
                        and frame[0] == CMD_CONFIRM_PORT
                        and frame[3] == CMD_SEND_ALL_SETTINGS
                        and frame[4] == 0x01):
                    confirmed = True
                    break

        out: dict[int, bytes] = {}
        for frame in self.rx_buffer[pre_len:]:
            out.update(parse_settings_response(frame))
        return out

    async def apply_profile_and_region(
        self, target_region: Optional[int], target_profile: Optional[dict]
    ) -> dict:
        """
        Write region and/or profile settings, skipping no-ops. Returns a summary
        dict for logging. Does NOT decide whether to reboot -- caller always reboots.
        """
        summary: dict = {
            "region_before": None,
            "region_after": None,
            "region_written": False,
            "profile_id": target_profile["id"] if target_profile else None,
            "settings_written": [],
            "settings_already_correct": [],
        }

        if target_region is not None:
            current = await self.read_region()
            summary["region_before"] = current
            if current != target_region:
                await self.write_region(target_region)
                summary["region_written"] = True
            summary["region_after"] = target_region

        if target_profile is not None:
            for name, sid, value in _profile_steps(target_profile):
                wrote = await self.write_setting_if_different(name, sid, value)
                if wrote:
                    summary["settings_written"].append(name)
                else:
                    summary["settings_already_correct"].append(name)

        return summary

    async def send_join(self) -> None:
        """Fire cmd_join (0xA0). Often a no-op when lr_joined is true; prefer send_reset."""
        await self._write(bytes([PORT_COMMANDS, CMD_JOIN, 0x00]))
        await asyncio.sleep(0.3)

    async def send_reset(self) -> None:
        """Fire cmd_reset (0xA1). Wipes RAM session, forces fresh OTAA on persisted region."""
        await self._write(bytes([PORT_COMMANDS, CMD_RESET, 0x00]))
        await asyncio.sleep(0.3)


# ---------------------------------------------------------------------------
# Per-device onboarding
# ---------------------------------------------------------------------------

async def _apply_post_action(
    session: "DeviceSession", post_action: str, dev_eui: str
) -> Optional[str]:
    """
    Fire the post-action and return its ISO timestamp (or None if skipped).

    Default is 'reboot' (cmd_reset). The script always reboots because the
    device's lr_joined flag is unreliable -- see module docstring.
    """
    if post_action == "none":
        return None
    try:
        if post_action == "rejoin":
            print(f"  Sending cmd_join (0xA0) -- light kick (often a no-op)...")
            await session.send_join()
        else:  # 'reboot' (default)
            print(f"  Sending cmd_reset (0xA1) -- forces fresh OTAA join...")
            await session.send_reset()
        return datetime.now(timezone.utc).isoformat()
    except Exception as e:
        print(f"  [WARN] {dev_eui}: post-action {post_action!r} failed: {type(e).__name__}: {e}")
        return None


async def _onboard_one_attempt(
    device: BLEDevice,
    adv_name: str,
    ledger: Ledger,
    args: argparse.Namespace,
    target_region: Optional[int],
    target_profile: Optional[dict],
    pre_run_addrs: dict[str, Optional[str]],
    target_dev_euis: Optional[set[str]] = None,
) -> tuple[str, Optional[str]]:
    """One onboarding attempt. Caller wraps this with retry + timeout."""
    label = adv_name or device.address
    async with DeviceSession(device) as session:
        dev_eui = await session.read_dev_eui()
        if not dev_eui:
            print(f"  [FAIL] {label}: could not read DevEUI")
            return "failed", None

        print(f"  DevEUI: {dev_eui}")

        if target_dev_euis is not None and dev_eui not in target_dev_euis:
            print(f"  [SKIP] {dev_eui}: not in --dev-eui filter")
            return "not-targeted", dev_eui

        # Pre-flight skip rule:
        #   1. If ChirpStack confirms the device is activated -> skip (don't
        #      disturb a working session).
        #   2. If ChirpStack was queried successfully but does NOT show this
        #      device as activated -> process it, even if the ledger claims
        #      we onboarded it before. The LNS is the authoritative source of
        #      truth; a "ledger says ok" without an LNS activation just means
        #      a previous run wrote the wrong settings (e.g. wrong region) or
        #      the device's join never reached the network.
        #   3. If we have NO LNS data (--no-verify, snapshot failed, etc.),
        #      fall back to the ledger as the only signal we have.
        # --force bypasses everything.
        if not args.force:
            pre_addr = pre_run_addrs.get(dev_eui.upper())
            if pre_addr:
                print(f"  [SKIP] {dev_eui}: already activated on ChirpStack "
                      f"(devAddr={pre_addr}). Use --force to re-onboard.")
                return "skipped-joined", dev_eui

            if not pre_run_addrs:
                # Case 3: no LNS truth available, fall back to ledger.
                if ledger.already_done(dev_eui):
                    print(f"  [SKIP] {dev_eui}: already onboarded in ledger "
                          f"(no LNS data this run; use --force to re-run)")
                    return "skipped", dev_eui
            elif ledger.already_done(dev_eui):
                # Case 2: LNS says not activated, but ledger says we did it.
                # Ledger is stale -- proceed with re-onboarding.
                print(f"  Note: ledger marks this device onboarded, but ChirpStack "
                      f"shows no activation. Re-onboarding (LNS is authoritative).")

        if args.dry_run:
            region_before = await session.read_region()
            print(f"  [DRY] current region={region_before}; would set "
                  f"region={target_region}, profile={target_profile['id'] if target_profile else 'none'}, "
                  f"then post-action={args.post_action}")
            return "dry-run", dev_eui

        summary = await session.apply_profile_and_region(target_region, target_profile)

        if summary["region_written"]:
            print(f"  [WRITE] lr_region {summary['region_before']} -> "
                  f"{summary['region_after']} ({REGIONS.get(summary['region_after'], '?')})")
        elif target_region is not None:
            print(f"  lr_region already {summary['region_after']} "
                  f"({REGIONS.get(summary['region_after'], '?')})")
        if summary["settings_written"]:
            print(f"  [WRITE] profile '{summary['profile_id']}': "
                  f"{', '.join(summary['settings_written'])}")
        if summary["settings_already_correct"]:
            print(f"  Already-correct profile settings: "
                  f"{', '.join(summary['settings_already_correct'])}")

        # Always set init_time to host clock (cheap, fixes stale 2021 timestamps
        # we observed on factory units; helps GPS cold-start).
        init_time_written: Optional[int] = None
        if not args.no_set_time:
            init_time_written = await session.write_init_time()
            print(f"  [WRITE] init_time = {init_time_written} "
                  f"({datetime.fromtimestamp(init_time_written, timezone.utc).isoformat()})")

        # Optional: set gps_init_lat/lon hint for faster GPS first-fix.
        home_lat = getattr(args, "home_lat", None)
        home_lon = getattr(args, "home_lon", None)
        gps_home_written = False
        if home_lat is not None and home_lon is not None:
            lat_w, lon_w = await session.write_gps_home(home_lat, home_lon)
            gps_home_written = lat_w or lon_w
            if gps_home_written:
                print(f"  [WRITE] gps_init = lat={home_lat:.6f}, lon={home_lon:.6f}")
            else:
                print(f"  gps_init already at lat={home_lat:.6f}, lon={home_lon:.6f}")

        reset_at = await _apply_post_action(session, args.post_action, dev_eui)

        ledger.record(dev_eui, {
            "ble_address": device.address,
            "ble_name": adv_name,
            "region_before": summary["region_before"],
            "region_after": summary["region_after"],
            "profile_applied": summary["profile_id"],
            "settings_written": summary["settings_written"],
            "init_time_written": init_time_written,
            "gps_home": {"lat": home_lat, "lon": home_lon} if home_lat is not None else None,
            "post_action": args.post_action,
            "reset_at": reset_at,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "ok",
        })
        print(f"  [OK] {dev_eui}")
        return "onboarded", dev_eui


async def onboard_one(
    device: BLEDevice,
    adv_name: str,
    ledger: Ledger,
    args: argparse.Namespace,
    target_region: Optional[int],
    target_profile: Optional[dict],
    pre_run_addrs: dict[str, Optional[str]],
    target_dev_euis: Optional[set[str]] = None,
) -> tuple[str, Optional[str]]:
    """
    Returns (outcome, dev_eui). Outcome is one of:
        'onboarded', 'skipped', 'skipped-joined', 'dry-run', 'failed', 'not-targeted'.

    Retries once on BleakError (transient BLE disconnect during connect/discovery is
    common in dense BLE environments and usually succeeds on a second try).
    """
    label = adv_name or device.address
    timeout = float(args.per_device_timeout)
    last_err: Optional[BaseException] = None

    for attempt in range(2):
        try:
            async with asyncio.timeout(timeout):
                return await _onboard_one_attempt(
                    device, adv_name, ledger, args, target_region, target_profile,
                    pre_run_addrs, target_dev_euis,
                )
        except asyncio.TimeoutError as e:
            print(f"  [FAIL] {label}: per-device timeout after {timeout:.0f}s")
            return "failed", None
        except BleakError as e:
            last_err = e
            if attempt == 0:
                print(f"  [RETRY] {label}: {type(e).__name__}: {e} -- retrying in 2s...")
                await asyncio.sleep(2.0)
                continue
            print(f"  [FAIL] {label}: {type(e).__name__}: {e} (after retry)")
            return "failed", None
        except Exception as e:
            print(f"  [FAIL] {label}: {type(e).__name__}: {e}")
            return "failed", None

    # Defensive fallthrough -- should never be reached.
    print(f"  [FAIL] {label}: {type(last_err).__name__}: {last_err}")
    return "failed", None


# ---------------------------------------------------------------------------
# Scan loop
# ---------------------------------------------------------------------------

async def scan_for_irnas(
    seen_this_run: set[str],
    sp_filter: Optional[set[str]] = None,
    duration: float = DEFAULT_SCAN_WINDOW_SEC,
) -> tuple[list[tuple[BLEDevice, str]], dict]:
    """One scan window. Returns (new_devices, stats)."""
    new_devices: list[tuple[BLEDevice, str]] = []
    irnas_addrs_this_window: set[str] = set()
    nameful_addrs: set[str] = set()

    def detection(device: BLEDevice, adv: AdvertisementData) -> None:
        if IRNAS_COMPANY_ID not in adv.manufacturer_data:
            return
        irnas_addrs_this_window.add(device.address)
        adv_name = adv.local_name or device.name
        if adv_name:
            nameful_addrs.add(device.address)
        if device.address in seen_this_run:
            return
        if sp_filter is not None:
            if not adv_name or adv_name.upper() not in sp_filter:
                return
        seen_this_run.add(device.address)
        new_devices.append((device, adv_name or "(unnamed)"))

    scanner = BleakScanner(detection_callback=detection)
    await scanner.start()
    await asyncio.sleep(duration)
    await scanner.stop()

    no_name_count = len(irnas_addrs_this_window - nameful_addrs)
    return new_devices, {
        "irnas_seen": len(irnas_addrs_this_window),
        "matched": len(new_devices),
        "no_name_addrs": no_name_count,
    }


async def list_mode(args: argparse.Namespace) -> int:
    """Diagnostic: scan and print every IRNAS advertisement seen, no connects."""
    print(f"List mode: scanning for IRNAS adverts (company ID 0x{IRNAS_COMPANY_ID:04X}).")
    print("Press Ctrl+C to stop.\n")
    print(f"{'SP#':<14} {'Address':<40} RSSI")
    print("-" * 66)

    seen: dict[str, tuple[str, int]] = {}
    stopping = False

    def _handle_sig(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, _handle_sig)

    def detection(device: BLEDevice, adv: AdvertisementData) -> None:
        if IRNAS_COMPANY_ID not in adv.manufacturer_data:
            return
        name = adv.local_name or device.name or "(no name)"
        rssi = adv.rssi if adv.rssi is not None else 0
        prev = seen.get(device.address)
        if prev is None or (prev[0] == "(no name)" and name != "(no name)"):
            seen[device.address] = (name, rssi)
            print(f"{name:<14} {device.address:<40} {rssi:>4} dBm")
        else:
            seen[device.address] = (prev[0], rssi)

    scanner = BleakScanner(detection_callback=detection)
    await scanner.start()
    while not stopping:
        await asyncio.sleep(0.5)
    await scanner.stop()

    print(f"\nTotal unique IRNAS devices seen: {len(seen)}")
    no_name = sum(1 for name, _ in seen.values() if name == "(no name)")
    if no_name:
        print(f"  (of which {no_name} never sent a complete-local-name adv packet)")
    return 0


# ---------------------------------------------------------------------------
# Argument helpers
# ---------------------------------------------------------------------------

def normalize_dev_eui(s: str) -> str:
    cleaned = s.replace(":", "").replace("-", "").replace(" ", "").strip().upper()
    if len(cleaned) != 16 or any(c not in "0123456789ABCDEF" for c in cleaned):
        raise argparse.ArgumentTypeError(f"DevEUI must be 16 hex characters (got {s!r})")
    return cleaned


def normalize_sp(s: str) -> str:
    cleaned = s.strip().upper().replace(",", "")
    if cleaned.startswith("SP"):
        cleaned = cleaned[2:]
    if not cleaned or not cleaned.isdigit():
        raise argparse.ArgumentTypeError(f"SP number must be digits or 'SP' + digits (got {s!r})")
    return f"SP{cleaned}"


def parse_region_arg(s: str) -> int:
    try:
        v = int(s)
    except ValueError:
        # accept name like "US915"
        for k, name in REGIONS.items():
            if name.upper() == s.upper():
                return k
        raise argparse.ArgumentTypeError(f"Unknown region {s!r}")
    if v not in REGIONS:
        raise argparse.ArgumentTypeError(f"Region must be 1-13 (got {v})")
    return v


# ---------------------------------------------------------------------------
# LNS (ChirpStack) config + verification
# ---------------------------------------------------------------------------

def load_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return out


def obscure_secret(s: str, keep_start: int = 6, keep_end: int = 4) -> str:
    """Show only the first/last few characters of a secret for display purposes."""
    if not s:
        return "(empty)"
    if len(s) <= keep_start + keep_end + 3:
        return "***"
    return f"{s[:keep_start]}...{s[-keep_end:]}"


def save_last_batch(path: Path, region: Optional[int],
                    profile_id: Optional[str], force: bool,
                    home_lat: Optional[float] = None,
                    home_lon: Optional[float] = None) -> None:
    """Save the wizard's answers for reuse on the next run."""
    data = {
        "version": 2,
        "region": region,
        "profile_id": profile_id,
        "force": force,
        "home_lat": home_lat,
        "home_lon": home_lon,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(data, indent=2))


def load_last_batch(path: Path) -> Optional[dict]:
    """
    Load saved wizard answers. Validates each field; returns None if the file
    is missing, malformed, or references a region/profile that no longer exists.
    Supports v1 (no home coords) and v2 (with home coords) on disk.
    """
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    if data.get("version") not in (1, 2):
        return None
    region = data.get("region")
    if region is not None and region not in REGIONS:
        return None
    profile_id = data.get("profile_id")
    if profile_id is not None and profile_id not in PROFILES_BY_ID:
        return None
    home_lat = data.get("home_lat")
    home_lon = data.get("home_lon")
    if home_lat is not None and not isinstance(home_lat, (int, float)):
        return None
    if home_lon is not None and not isinstance(home_lon, (int, float)):
        return None
    return {
        "region": region,
        "profile_id": profile_id,
        "force": bool(data.get("force", False)),
        "home_lat": home_lat,
        "home_lon": home_lon,
        "saved_at": data.get("saved_at"),
    }


def save_env_file(path: Path, cfg: dict) -> None:
    """Write CHIRPSTACK_* vars to a .env file with restrictive permissions."""
    body = (
        "# ChirpStack credentials for batch-onboard-us915.py\n"
        "# Generated by the wizard. Safe to edit by hand.\n"
        "# Keep this file out of git; the directory's .gitignore should already exclude it.\n"
        f"CHIRPSTACK_BASE_URL={cfg['base_url']}\n"
        f"CHIRPSTACK_API_KEY={cfg['api_key']}\n"
        f"CHIRPSTACK_APPLICATION_ID={cfg['app_id']}\n"
    )
    path.write_text(body)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def load_lns_config(args: argparse.Namespace) -> Optional[dict]:
    """
    Resolve ChirpStack credentials. Priority:
      1. CLI flags (--lns-url, --lns-api-key, --lns-app-id)
      2. Env file (--lns-env PATH, or default ./.env next to the script)
      3. Process environment variables (CHIRPSTACK_BASE_URL etc.)
      4. Interactive questionary prompts (only if interactive AND --no-verify not set)

    If all three are gathered interactively, offers to save them to ./.env.
    Returns dict with base_url/api_key/app_id, or None if unresolved.
    """
    env_path = Path(args.lns_env) if args.lns_env else LOCAL_ENV_PATH
    env = load_env_file(env_path) if env_path.exists() else {}

    cfg = {
        "base_url": args.lns_url or env.get("CHIRPSTACK_BASE_URL") or os.environ.get("CHIRPSTACK_BASE_URL"),
        "api_key":  args.lns_api_key or env.get("CHIRPSTACK_API_KEY") or os.environ.get("CHIRPSTACK_API_KEY"),
        "app_id":   args.lns_app_id or env.get("CHIRPSTACK_APPLICATION_ID") or os.environ.get("CHIRPSTACK_APPLICATION_ID"),
    }

    if all(cfg.values()):
        return cfg

    if args.no_verify:
        return None

    # Interactive fill-in. Pre-fill any partial value as the prompt default
    # so the user can press Enter to accept what's already known.
    if questionary is None or not sys.stdin.isatty():
        return None

    print("\nChirpStack credentials are needed for pre-flight skip and post-flight verification.")
    print("Press Ctrl-C at any prompt to skip verification (you can still onboard devices).\n")
    labels = [
        ("base_url", "ChirpStack URL (e.g. https://console.example.com)", False),
        ("app_id",   "ChirpStack application UUID", False),
        ("api_key",  "ChirpStack API key", True),
    ]
    for key, label, is_password in labels:
        if cfg[key]:
            continue
        if is_password:
            answer = questionary.password(label).ask()
        else:
            answer = questionary.text(label).ask()
        if answer is None:  # user cancelled
            return None
        cfg[key] = answer.strip() or None

    if not all(cfg.values()):
        return None

    # Offer to save for next time
    if not env_path.exists():
        save = questionary.confirm(
            f"Save these credentials to {env_path} for future runs? "
            f"(file will be created with mode 0600)",
            default=True,
        ).ask()
        if save:
            try:
                save_env_file(env_path, cfg)
                print(f"  Saved to {env_path}")
            except OSError as e:
                print(f"  [WARN] Could not save: {e}")

    return cfg


def _http_get_json(url: str, api_key: str, timeout: float = 15.0) -> Optional[dict]:
    """Synchronous GET returning parsed JSON, or None on any error."""
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except Exception:
        return None


def query_activation(lns_cfg: dict, dev_eui: str) -> Optional[str]:
    """
    Returns the device's current devAddr (string) if it has an activation,
    or None if it has never joined / activation is empty.
    """
    url = f"{lns_cfg['base_url'].rstrip('/')}/api/devices/{dev_eui.lower()}/activation"
    data = _http_get_json(url, lns_cfg["api_key"])
    if not data:
        return None
    activation = data.get("deviceActivation")
    if not activation:
        return None
    devaddr = activation.get("devAddr")
    return devaddr if devaddr else None


async def snapshot_activations(lns_cfg: dict, dev_euis: list[str]) -> dict[str, Optional[str]]:
    """
    Query the activation endpoint for each DevEUI in parallel.
    Returns {dev_eui_upper: devAddr_or_None}.
    """
    from concurrent.futures import ThreadPoolExecutor

    def _one(eui: str) -> tuple[str, Optional[str]]:
        return eui.upper(), query_activation(lns_cfg, eui)

    out: dict[str, Optional[str]] = {}
    loop = asyncio.get_event_loop()
    # Cap concurrency to avoid hammering the API.
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = await loop.run_in_executor(
            None, lambda: list(pool.map(_one, dev_euis))
        )
    for eui, devaddr in results:
        out[eui] = devaddr
    return out


async def list_app_dev_euis(lns_cfg: dict) -> list[str]:
    """Return all DevEUIs in the application (one paged GET)."""
    url = (f"{lns_cfg['base_url'].rstrip('/')}/api/devices"
           f"?applicationId={lns_cfg['app_id']}&limit=1000")
    data = _http_get_json(url, lns_cfg["api_key"], timeout=20.0)
    if not data:
        return []
    return [d["devEui"].upper() for d in data.get("result", [])]


async def verify_joins(
    lns_cfg: dict,
    ledger: Ledger,
    run_devices: list[str],
    pre_run_addrs: dict[str, Optional[str]],
    wait_min: int,
) -> None:
    """
    Wait `wait_min` minutes, then query ChirpStack activation for each
    touched device and compare against the pre-run snapshot.

    Outcomes:
      - JOINED (new this run):  no activation before, has activation now
      - JOINED (re-joined):     activation before, devAddr changed
      - ALREADY JOINED:         activation before, devAddr unchanged (no fresh JR observed)
      - NOT JOINED:             no activation now (regardless of before)

    `lastSeenAt` is intentionally NOT used: it does not update on JoinRequest
    in ChirpStack v4, only on data uplinks, which can be hours away on a
    freshly-joined device.
    """
    if not run_devices:
        print("\nNo devices to verify (none touched this run).")
        return

    print(f"\nWaiting {wait_min} min for {len(run_devices)} device(s) to join, "
          f"then checking ChirpStack at {lns_cfg['base_url']}...")
    for remaining in range(wait_min, 0, -1):
        sys.stdout.write(f"\r  {remaining} min remaining...   ")
        sys.stdout.flush()
        await asyncio.sleep(60)
    sys.stdout.write("\r                                  \r")
    sys.stdout.flush()

    print(f"Querying ChirpStack activations for {len(run_devices)} device(s)...")
    post_run_addrs = await snapshot_activations(lns_cfg, run_devices)

    new_join: list[tuple[str, str, str]] = []        # (sp, eui, devaddr)
    rejoined: list[tuple[str, str, str, str]] = []   # (sp, eui, before, after)
    already: list[tuple[str, str, str]] = []         # (sp, eui, devaddr) -- joined but no fresh JR
    not_joined: list[tuple[str, str]] = []           # (sp, eui)

    for dev_eui in run_devices:
        entry = ledger.data["devices"].get(dev_eui, {})
        sp = entry.get("ble_name", "?")
        before = pre_run_addrs.get(dev_eui.upper())
        after = post_run_addrs.get(dev_eui.upper())
        if not after:
            not_joined.append((sp, dev_eui))
        elif not before:
            new_join.append((sp, dev_eui, after))
        elif before == after:
            already.append((sp, dev_eui, after))
        else:
            rejoined.append((sp, dev_eui, before, after))

    joined_count = len(new_join) + len(rejoined)
    print(f"\n=== Network verification ({wait_min} min after last reboot) ===")
    print(f"  JOINED this run:        {joined_count}/{len(run_devices)}  "
          f"({len(new_join)} new, {len(rejoined)} re-joined with new devAddr)")
    if already:
        print(f"  ALREADY JOINED:         {len(already)}/{len(run_devices)}  "
              f"(activation unchanged since before this run -- no new JR observed)")
    print(f"  NOT JOINED:             {len(not_joined)}/{len(run_devices)}  "
          f"(no activation in ChirpStack)")

    if new_join:
        print("\n  Joined for the first time this run:")
        for sp, eui, devaddr in sorted(new_join):
            print(f"    {sp:<10} {eui}  devAddr={devaddr}")
    if rejoined:
        print("\n  Re-joined this run (new devAddr):")
        for sp, eui, before, after in sorted(rejoined):
            print(f"    {sp:<10} {eui}  {before} -> {after}")
    if already:
        print("\n  Activation present but devAddr unchanged:")
        for sp, eui, devaddr in sorted(already):
            print(f"    {sp:<10} {eui}  devAddr={devaddr} "
                  f"(was joined before; cmd_reset may not have triggered a new JR)")
    if not_joined:
        print("\n  Did NOT join (no activation in ChirpStack):")
        for sp, eui in sorted(not_joined):
            print(f"    {sp:<10} {eui}")
        print("\n  Hints: confirm the device is registered in ChirpStack; check "
              "gateway coverage; re-run with --force.")


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------

async def run(args: argparse.Namespace) -> int:
    ledger = Ledger(LEDGER_PATH)
    print(f"Ledger: {LEDGER_PATH} ({ledger.count_ok()} devices already onboarded)")

    target_region: Optional[int] = getattr(args, "target_region", None)
    target_profile: Optional[dict] = getattr(args, "target_profile", None)

    print(f"Target region: {REGIONS.get(target_region, 'no change') if target_region else 'no change'}"
          f"{f' ({target_region})' if target_region else ''}")
    print(f"Target profile: {target_profile['name'] if target_profile else 'no change'}")
    print(f"Post-action: {args.post_action}")
    if args.dry_run:
        print("DRY RUN -- will identify devices but not write or reboot.")

    sp_filter: Optional[set[str]] = set(args.sp) if args.sp else None
    if sp_filter:
        print(f"SP filter: {len(sp_filter)} SP number(s).")
        if len(sp_filter) <= 20:
            for sp in sorted(sp_filter):
                print(f"  - {sp}")

    dev_eui_filter: Optional[set[str]] = set(args.dev_eui) if args.dev_eui else None
    if dev_eui_filter:
        print(f"DevEUI validation (post-connect): {len(dev_eui_filter)} DevEUI(s)")

    if sp_filter is None and dev_eui_filter is None and not getattr(args, "_wizard_confirmed", False):
        print("[WARN] No --sp or --dev-eui filter set. Will attempt every IRNAS device in range.")

    # Pre-run activation snapshot: capture each app device's current devAddr (if any)
    # so we can later distinguish "joined this run" (new/changed devAddr) from
    # "already joined before" (devAddr unchanged) from "did not join" (still no devAddr).
    pre_run_addrs: dict[str, Optional[str]] = {}
    lns_cfg: Optional[dict] = getattr(args, "_lns_cfg", None)
    if not args.no_verify and lns_cfg and not args.dry_run:
        print(f"\nCapturing pre-run activation snapshot from ChirpStack...")
        try:
            app_euis = await list_app_dev_euis(lns_cfg)
            if app_euis:
                pre_run_addrs = await snapshot_activations(lns_cfg, app_euis)
                joined_now = sum(1 for v in pre_run_addrs.values() if v)
                print(f"  Snapshot: {len(app_euis)} devices in app, {joined_now} currently activated.")
            else:
                print("  [WARN] No devices found in application; verification will treat all as new joins.")
        except Exception as e:
            print(f"  [WARN] Pre-run snapshot failed: {type(e).__name__}: {e}. "
                  f"Verification will still run but cannot distinguish 'already joined'.")
    print()

    stopping = False

    def _handle_sig(*_):
        nonlocal stopping
        stopping = True
        print("\n[INTERRUPT] Finishing current device then exiting...")

    signal.signal(signal.SIGINT, _handle_sig)

    seen_this_run: set[str] = set()
    handled_sps: set[str] = set()
    handled_dev_euis: set[str] = set()
    devices_touched: list[str] = []  # DevEUIs that we successfully onboarded this run
    failed_devices: list[tuple[str, str]] = []  # (label, address) for devices that failed
    tallies = {"onboarded": 0, "skipped": 0, "skipped-joined": 0,
               "dry-run": 0, "failed": 0, "not-targeted": 0}
    empty_windows = 0
    window_num = 0

    def all_targets_handled() -> bool:
        if sp_filter is not None and handled_sps < sp_filter:
            return False
        if dev_eui_filter is not None and handled_dev_euis < dev_eui_filter:
            return False
        return (sp_filter is not None) or (dev_eui_filter is not None)

    while not stopping:
        if all_targets_handled():
            print("[scan] all targets handled, exiting.")
            break

        window_num += 1
        found, stats = await scan_for_irnas(seen_this_run, sp_filter, args.scan_seconds)
        bits = []
        if sp_filter is not None:
            bits.append(f"{len(sp_filter - handled_sps)} SP#s pending")
        if dev_eui_filter is not None:
            bits.append(f"{len(dev_eui_filter - handled_dev_euis)} DevEUIs pending")
        suffix = (", " + ", ".join(bits)) if bits else ""
        no_name = (f", {stats['no_name_addrs']} without adv name"
                   if stats["no_name_addrs"] else "")
        print(f"[scan] window {window_num}: {stats['irnas_seen']} IRNAS adverts in range, "
              f"{stats['matched']} new match(es){no_name}{suffix}")

        if not found:
            empty_windows += 1
            if empty_windows >= args.idle_windows:
                print(f"[scan] {args.idle_windows} empty windows, exiting.")
                break
            continue

        empty_windows = 0
        for device, adv_name in found:
            if stopping:
                break
            print(f"\n--> {adv_name} @ {device.address}")
            outcome, dev_eui = await onboard_one(
                device, adv_name, ledger, args, target_region, target_profile,
                pre_run_addrs, target_dev_euis=dev_eui_filter,
            )
            tallies[outcome] = tallies.get(outcome, 0) + 1
            if outcome == "onboarded" and dev_eui:
                devices_touched.append(dev_eui)
            if outcome == "failed":
                failed_devices.append((adv_name or "(unnamed)", device.address))
            if outcome != "not-targeted":
                if sp_filter is not None and adv_name.upper() in sp_filter:
                    handled_sps.add(adv_name.upper())
                if dev_eui_filter is not None and dev_eui and dev_eui in dev_eui_filter:
                    handled_dev_euis.add(dev_eui)
            if all_targets_handled():
                print("\n[scan] all targets handled, exiting.")
                stopping = True
                break
            await asyncio.sleep(INTER_DEVICE_PAUSE_SEC)

    print("\n" + "=" * 48)
    print("Summary")
    print("=" * 48)
    print(f"  Onboarded this run:     {tallies['onboarded']}")
    if tallies["skipped-joined"]:
        print(f"  Skipped (LNS active):   {tallies['skipped-joined']}  (already activated on ChirpStack)")
    print(f"  Skipped (ledger):       {tallies['skipped']}")
    if tallies["dry-run"]:
        print(f"  Dry-run hits:           {tallies['dry-run']}")
    if tallies["not-targeted"]:
        print(f"  DevEUI validation miss: {tallies['not-targeted']}")
    print(f"  Failed:                 {tallies['failed']}")
    print(f"  Ledger total OK:        {ledger.count_ok()}")

    if failed_devices:
        print(f"\n  Failed devices ({len(failed_devices)}):")
        for label, addr in failed_devices:
            print(f"    {label:<14} @ {addr}")
        print("\n  Hint: re-run with the same args to retry these "
              "(they're not in the ledger yet, so they will be re-attempted automatically).")

    missing_sps = sp_filter - handled_sps if sp_filter else set()
    missing_euis = dev_eui_filter - handled_dev_euis if dev_eui_filter else set()
    if missing_sps:
        print(f"\n  [WARN] SP#s never matched in scan ({len(missing_sps)}):")
        for sp in sorted(missing_sps)[:20]:
            print(f"    - {sp}")
    if missing_euis:
        print(f"\n  [WARN] DevEUIs never matched ({len(missing_euis)}):")
        for eui in sorted(missing_euis)[:20]:
            print(f"    - {eui}")

    # Post-flight ChirpStack verification
    if not args.no_verify and lns_cfg and devices_touched and not args.dry_run:
        await verify_joins(lns_cfg, ledger, devices_touched, pre_run_addrs, args.verify_wait_min)
    elif not args.no_verify and devices_touched and not args.dry_run:
        print("\n[INFO] Skipping ChirpStack verification (no LNS config). "
              "Use check-onboarded-status.py later to verify joins.")

    rc = 0 if tallies["failed"] == 0 and not missing_sps and not missing_euis else 1
    return rc


async def run_by_address(args: argparse.Namespace) -> int:
    """Direct-connect mode: skip scanning and talk to one specific peripheral."""
    ledger = Ledger(LEDGER_PATH)
    print(f"Ledger: {LEDGER_PATH} ({ledger.count_ok()} devices already onboarded)")
    print(f"Direct-connect mode: {args.by_address}\n")

    target_region: Optional[int] = getattr(args, "target_region", None)
    target_profile: Optional[dict] = getattr(args, "target_profile", None)
    lns_cfg: Optional[dict] = getattr(args, "_lns_cfg", None)

    # Pre-snapshot the entire app's activations BEFORE we touch the device,
    # so verification can distinguish new joins from existing activations.
    pre_run_addrs: dict[str, Optional[str]] = {}
    if not args.no_verify and lns_cfg and not args.dry_run:
        try:
            app_euis = await list_app_dev_euis(lns_cfg)
            if app_euis:
                pre_run_addrs = await snapshot_activations(lns_cfg, app_euis)
        except Exception as e:
            print(f"[WARN] Pre-run snapshot failed: {type(e).__name__}: {e}")

    print(f"Looking up peripheral via a {BY_ADDRESS_LOOKUP_SEC:.0f}s scan...")
    device = await BleakScanner.find_device_by_address(args.by_address, timeout=BY_ADDRESS_LOOKUP_SEC)
    if device is None:
        print(f"[FAIL] No peripheral with address {args.by_address} seen in range.")
        return 1

    adv_name = device.name or "(unknown)"
    print(f"\n--> {adv_name} @ {device.address}")
    outcome, dev_eui = await onboard_one(
        device, adv_name, ledger, args, target_region, target_profile,
        pre_run_addrs, target_dev_euis=set(args.dev_eui) if args.dev_eui else None,
    )
    print(f"\nOutcome: {outcome}")

    if outcome == "onboarded" and dev_eui and lns_cfg and not args.no_verify and not args.dry_run:
        await verify_joins(lns_cfg, ledger, [dev_eui], pre_run_addrs, args.verify_wait_min)

    return 0 if outcome != "failed" else 1


# ---------------------------------------------------------------------------
# Interactive wizard
# ---------------------------------------------------------------------------

def _profile_label(p: dict) -> str:
    return f"{p['name']:<14} {p['description']}  (~{p['battery_days']}d battery)"


def _region_label(code: int) -> str:
    descriptions = {
        3: "United States, Canada, Mexico (Helium)",
        1: "Europe",
        2: "Asia (group 1)",
        4: "Australia",
    }
    desc = descriptions.get(code, "")
    return f"{REGIONS[code]:<12} ({code}){'  -- ' + desc if desc else ''}"


def _validate_coord_pair(text: str) -> Optional[tuple[float, float]]:
    """Parse 'lat,lon' into (float, float). Returns None for blank/invalid."""
    if not text or not text.strip():
        return None
    try:
        parts = [p.strip() for p in text.replace(";", ",").split(",")]
        if len(parts) != 2:
            return None
        lat = float(parts[0])
        lon = float(parts[1])
        if not -90.0 <= lat <= 90.0:
            return None
        if not -180.0 <= lon <= 180.0:
            return None
        return lat, lon
    except (ValueError, TypeError):
        return None


async def _ask_region_and_profile_and_force(
    args: argparse.Namespace,
) -> Optional[tuple[Optional[int], Optional[dict], bool, Optional[float], Optional[float]]]:
    """Run the four core wizard prompts. Returns None on cancel."""
    NO_CHANGE = "__no_change__"
    ADVANCED = "__advanced__"

    common_choices = [
        questionary.Choice(_region_label(c), value=c) for c in COMMON_REGIONS
    ] + [
        questionary.Choice("(no change)  -- leave whatever each device currently has",
                           value=NO_CHANGE),
        questionary.Choice("Other / advanced (show all 13 Semtech regions)", value=ADVANCED),
    ]
    region_choice = await asyncio.to_thread(
        questionary.select("Which spectrum region should devices be set to?",
                           choices=common_choices, default=common_choices[0]).ask
    )
    if region_choice is None:
        return None
    if region_choice == ADVANCED:
        all_choices = [questionary.Choice(_region_label(c), value=c) for c in REGIONS]
        all_choices.append(questionary.Choice("(no change)", value=NO_CHANGE))
        region_choice = await asyncio.to_thread(
            questionary.select("Pick a region:", choices=all_choices).ask
        )
        if region_choice is None:
            return None
    region: Optional[int] = None if region_choice == NO_CHANGE else region_choice

    profile_choices = [
        questionary.Choice("None  -- leave intervals at factory defaults", value=NO_CHANGE),
    ] + [
        questionary.Choice(_profile_label(p), value=p["id"]) for p in UPLINK_PROFILES
    ]
    profile_choice = await asyncio.to_thread(
        questionary.select("Apply a quick profile (GPS + status uplink intervals)?",
                           choices=profile_choices, default=profile_choices[0]).ask
    )
    if profile_choice is None:
        return None
    profile = PROFILES_BY_ID.get(profile_choice) if profile_choice != NO_CHANGE else None

    revisit = await asyncio.to_thread(
        questionary.confirm(
            "Force re-visit devices that are already activated on ChirpStack? "
            "(Recommended: No -- skipping LNS-active devices saves time and "
            "avoids disturbing working sessions. Stale ledger entries are "
            "ignored automatically when ChirpStack data is available.)",
            default=False).ask
    )
    if revisit is None:
        return None

    # GPS home hint -- written to gps_init_lat/lon to speed up first GPS fix
    # at the deployment site. Skipping is safe; the device just takes longer
    # to acquire its first fix.
    home_lat: Optional[float] = None
    home_lon: Optional[float] = None
    home_text = await asyncio.to_thread(
        questionary.text(
            "GPS home for first-fix hint (decimal lat,lon -- e.g. '30.0,-115.5'). "
            "Set to your deployment area, NOT your current location. Press Enter "
            "to skip.",
            validate=lambda t: True if not t.strip() or _validate_coord_pair(t)
                              else "Use 'lat,lon' with -90<=lat<=90 and -180<=lon<=180").ask
    )
    if home_text is None:
        return None
    parsed = _validate_coord_pair(home_text)
    if parsed:
        home_lat, home_lon = parsed

    return region, profile, bool(revisit), home_lat, home_lon


def _format_saved_summary(lns_cfg: Optional[dict], saved: dict) -> str:
    """Pretty-print the saved settings for the reuse-confirm prompt."""
    region = saved.get("region")
    region_str = f"{REGIONS[region]} ({region})" if region else "no change"
    profile_id = saved.get("profile_id")
    profile_str = (PROFILES_BY_ID[profile_id]["name"]
                   + f"  -- {PROFILES_BY_ID[profile_id]['description']}"
                   if profile_id else "no change")
    force_str = "yes (force re-visit)" if saved.get("force") else "no (skip already-onboarded)"
    saved_at = saved.get("saved_at", "?")
    home_lat = saved.get("home_lat")
    home_lon = saved.get("home_lon")
    home_str = (f"lat={home_lat:.6f}, lon={home_lon:.6f}"
                if home_lat is not None and home_lon is not None else "skip")

    lines = [f"Last batch saved {saved_at}:"]
    if lns_cfg:
        lines.append(f"  ChirpStack:  {lns_cfg['base_url']}")
        lines.append(f"               app={lns_cfg['app_id']}")
        lines.append(f"               key={obscure_secret(lns_cfg['api_key'])}")
    lines.append(f"  Region:      {region_str}")
    lines.append(f"  Profile:     {profile_str}")
    lines.append(f"  Force:       {force_str}")
    lines.append(f"  GPS home:    {home_str}")
    return "\n".join(lines)


async def run_wizard(args: argparse.Namespace) -> int:
    if questionary is None:
        print("[ERROR] questionary is not installed. Either install it "
              "(pip install -r requirements.txt) or run with explicit flags "
              "to bypass the wizard. Run with --help for options.")
        return 2

    print("\n" + "=" * 60)
    print(" IRNAS batch onboarding wizard")
    print("=" * 60)
    print("This wizard will scan for IRNAS devices in BLE range, optionally")
    print("apply a region + quick profile, then cmd_reset every device so")
    print("the radio re-initializes and a fresh JoinRequest is transmitted.")
    print()

    # 1. LNS credentials FIRST (silent if .env present, prompts otherwise).
    #    Doing this up-front lets us include them in the saved-batch summary.
    lns_cfg = load_lns_config(args)
    if lns_cfg:
        print(f"Using ChirpStack at {lns_cfg['base_url']} "
              f"(application {lns_cfg['app_id']}).\n")
    else:
        print("[WARN] No ChirpStack credentials. Post-flight verification "
              "will be skipped. Use check-onboarded-status.py later to verify.\n")
        args.no_verify = True
    args._lns_cfg = lns_cfg

    # 2. Try to reuse last-batch answers.
    saved = load_last_batch(LAST_BATCH_PATH)
    region: Optional[int]
    profile: Optional[dict]
    answers_came_from_save = False

    if saved is not None:
        print(_format_saved_summary(lns_cfg, saved))
        print()
        reuse = await asyncio.to_thread(
            questionary.confirm("Reuse these settings?", default=True).ask
        )
        if reuse is None:
            print("Cancelled.")
            return 130
        if reuse:
            region = saved["region"]
            profile = PROFILES_BY_ID.get(saved["profile_id"]) if saved["profile_id"] else None
            args.force = bool(saved["force"])
            args.home_lat = saved.get("home_lat")
            args.home_lon = saved.get("home_lon")
            answers_came_from_save = True
            print("Using saved settings.\n")

    # 3. If we didn't reuse, run the prompts fresh.
    if not answers_came_from_save:
        result = await _ask_region_and_profile_and_force(args)
        if result is None:
            print("Cancelled.")
            return 130
        region, profile, force, home_lat, home_lon = result
        args.force = force
        args.home_lat = home_lat
        args.home_lon = home_lon

        # Offer to save this set as the new defaults (only when we asked fresh)
        save = await asyncio.to_thread(
            questionary.confirm(
                f"Save these answers to {LAST_BATCH_PATH.name} so the next run "
                f"can reuse them with a single y/n?", default=True).ask
        )
        if save:
            try:
                save_last_batch(LAST_BATCH_PATH, region,
                                profile["id"] if profile else None,
                                args.force, home_lat, home_lon)
                print(f"  Saved to {LAST_BATCH_PATH}\n")
            except OSError as e:
                print(f"  [WARN] Could not save: {e}\n")

    # Pre-scan: count devices in range
    print(f"\nScanning {args.scan_seconds:.0f}s for IRNAS devices in range...")
    _found, stats = await scan_for_irnas(set(), None, args.scan_seconds)
    print(f"Found {stats['irnas_seen']} IRNAS device(s) in range.")
    if stats["no_name_addrs"]:
        print(f"  ({stats['no_name_addrs']} of those did not advertise a complete local name)")

    # Final confirmation
    region_str = (f"{REGIONS[region]} ({region})" if region else "no change")
    profile_str = profile["name"] if profile else "no change"
    verify_str = (f"verify on ChirpStack after {args.verify_wait_min} min"
                  if lns_cfg and not args.no_verify else "no LNS verification")
    if args.force:
        skip_str = "no (--force: process every device)"
    elif lns_cfg and not args.no_verify:
        skip_str = "skip LNS-activated; ignore stale ledger entries (LNS is source of truth)"
    else:
        skip_str = "skip ledger-onboarded (LNS not queried; ledger is fallback)"

    home_str = (f"lat={args.home_lat:.6f}, lon={args.home_lon:.6f}"
                if args.home_lat is not None and args.home_lon is not None else "skip")
    set_time_str = "no (--no-set-time)" if args.no_set_time else "yes (host UTC)"

    print("\nPlan:")
    print(f"  Region:                {region_str}")
    print(f"  Profile:               {profile_str}")
    print(f"  GPS home (init):       {home_str}")
    print(f"  Set device clock:      {set_time_str}")
    print(f"  Skip behavior:         {skip_str}")
    print(f"  Post-action:           cmd_reset (always reboot, see module docstring)")
    print(f"  Verification:          {verify_str}")
    print(f"  Devices in range:      {stats['irnas_seen']}")

    # FYI countdown -- give the user a chance to Ctrl-C if anything looks wrong,
    # but don't make them press a key for the common case.
    print()
    countdown = 5
    try:
        for remaining in range(countdown, 0, -1):
            sys.stdout.write(f"\rStarting in {remaining}s... (Ctrl-C to cancel)   ")
            sys.stdout.flush()
            await asyncio.sleep(1)
        sys.stdout.write("\r" + " " * 50 + "\r")
        sys.stdout.flush()
    except (KeyboardInterrupt, asyncio.CancelledError):
        sys.stdout.write("\nCancelled.\n")
        return 130

    args.target_region = region
    args.target_profile = profile
    args._wizard_confirmed = True
    return await run(args)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Report mode (read-only QA snapshot)
# ---------------------------------------------------------------------------

# Schema files that, if present, give us setting name + decode hints. We look
# in a few places so the script works both standalone (OSS) and inside the
# original buoy-fish-tech monorepo. Missing schema is fine -- the report
# falls back to "unknown_0xXX" with raw hex for unrecognized IDs.
SCHEMA_CANDIDATE_PATHS = [
    SCRIPT_DIR / "settings_v7.0.0.json",
    SCRIPT_DIR / "settings-v7.0.0.json",
    SCRIPT_DIR.parent.parent / "prod" / "configure-buoy-fish" / "settings" / "settings_v7.0.0.json",
    SCRIPT_DIR.parent.parent / "prod" / "configure-buoy-fish" / "settings" / "settings-v7.0.0.json",
]
META_CANDIDATE_PATHS = [
    SCRIPT_DIR / "settings-meta.json",
    SCRIPT_DIR.parent.parent / "prod" / "configure-buoy-fish" / "settings-meta.json",
]

# Settings whose values are sensitive and should be redacted in reports.
SECRET_SETTING_NAMES: set[str] = {
    "app_key", "lp0_app_key", "lp0_network_key", "s_band_app_key",
    "s_band_network_key", "device_pin",
}


def load_settings_schema() -> dict[int, dict]:
    """
    Load {setting_id: {name, length, conversion}} from one of the candidate
    schema files. Returns an empty dict if none found (report still works,
    just without setting names).
    """
    for path in SCHEMA_CANDIDATE_PATHS:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        schema: dict[int, dict] = {}
        for name, meta in data.get("settings", {}).items():
            sid_raw = meta.get("id")
            if sid_raw is None:
                continue
            try:
                sid = int(sid_raw, 16) if isinstance(sid_raw, str) else int(sid_raw)
            except (ValueError, TypeError):
                continue
            schema[sid] = {
                "name": name,
                "length": meta.get("length"),
                "conversion": meta.get("conversion"),
            }
        return schema
    return {}


def load_settings_meta() -> dict[str, dict]:
    """Load human-readable descriptions from settings-meta.json (optional)."""
    for path in META_CANDIDATE_PATHS:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        # Top level may be a list or a dict depending on version
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return {entry["name"]: entry for entry in data if "name" in entry}
    return {}


def decode_setting_value(name: str, raw: bytes, conversion: Optional[str]) -> object:
    """Decode a raw setting value into a Python type based on its declared conversion."""
    if name in SECRET_SETTING_NAMES:
        return f"<redacted, {len(raw)} bytes>"
    if not raw:
        return None
    try:
        if conversion == "uint8":
            return raw[0]
        if conversion == "uint16":
            return int.from_bytes(raw, "little", signed=False)
        if conversion == "uint32":
            return int.from_bytes(raw, "little", signed=False)
        if conversion == "int32":
            return int.from_bytes(raw, "little", signed=True)
        if conversion == "bool":
            return raw[0] != 0
        if conversion == "string":
            return raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace")
        if conversion in ("hex", "byte_array", None):
            return raw.hex().upper()
        # Fallback: best guess by length
        if len(raw) == 1:
            return raw[0]
        if len(raw) in (2, 4):
            return int.from_bytes(raw, "little", signed=False)
        return raw.hex().upper()
    except Exception:
        return raw.hex().upper()


def _format_coord(scaled: object) -> str:
    """Format an int32-scaled coord as decimal degrees, with sign."""
    if not isinstance(scaled, int):
        return str(scaled)
    return f"{int_to_coord(scaled):+.6f}"


def _format_unix_ts(ts: object) -> str:
    """Format a Unix timestamp as ISO8601 UTC, or '<unset>' for 0."""
    if not isinstance(ts, int) or ts == 0:
        return "<unset / 0>"
    try:
        return datetime.fromtimestamp(ts, timezone.utc).isoformat()
    except (OSError, ValueError, OverflowError):
        return f"<invalid: {ts}>"


def _render_device_report_md(record: dict) -> str:
    """Render one device's record as a Markdown report."""
    lines = [
        f"# {record['ble_name']}  ({record['dev_eui']})",
        "",
        f"- **Generated:** {record['generated_at']}",
        f"- **BLE address:** `{record['ble_address']}`",
        f"- **Host:** {record['host']}",
        "",
    ]
    # Network status
    cs = record.get("chirpstack") or {}
    lines.append("## Network status")
    if not cs.get("queried"):
        lines.append("- ChirpStack not queried.")
    elif cs.get("devaddr"):
        lines.append(f"- **ACTIVATED**  devAddr=`{cs['devaddr']}`  fCntUp={cs.get('fcnt_up')}")
    elif cs.get("registered"):
        lines.append("- Registered in application but **NOT ACTIVATED** (no JoinRequest accepted yet).")
    else:
        lines.append("- Not registered in ChirpStack application.")
    lines.append("")

    # Highlight key LoRa settings
    decoded = record["settings_decoded"]
    region = decoded.get("lr_region")
    region_name = REGIONS.get(region, "?") if isinstance(region, int) else "?"
    lines += [
        "## LoRaWAN",
        f"- `lr_region`: **{region}** ({region_name})",
        f"- `device_eui`: `{decoded.get('device_eui', '?')}`",
        f"- `app_eui`: `{decoded.get('app_eui', '?')}`",
        f"- `app_key`: {decoded.get('app_key', '?')}",
        f"- `lr_send_flag`: {decoded.get('lr_send_flag', '?')}",
        f"- `lr_adr`: {decoded.get('lr_adr', '?')}  /  `lr_adr_profile`: {decoded.get('lr_adr_profile', '?')}",
        "",
    ]

    # GPS / profile
    lines += [
        "## GPS profile",
        f"- `lr_gps_interval`: {decoded.get('lr_gps_interval', '?')} s",
        f"- `ublox_send_interval`: {decoded.get('ublox_send_interval', '?')} s",
        f"- `ublox_send_interval_2`: {decoded.get('ublox_send_interval_2', '?')} s",
        f"- `ublox_multiple_intervals`: {decoded.get('ublox_multiple_intervals', '?')}",
        f"- `status_send_interval`: {decoded.get('status_send_interval', '?')} s",
        f"- `enable_motion_trig_gps`: {decoded.get('enable_motion_trig_gps', '?')}",
        f"- `motion_ths`: {decoded.get('motion_ths', '?')}",
        f"- `gps_init_lat`: {_format_coord(decoded.get('gps_init_lat'))}",
        f"- `gps_init_lon`: {_format_coord(decoded.get('gps_init_lon'))}",
        f"- `init_time`: {_format_unix_ts(decoded.get('init_time'))}",
        "",
    ]

    # Last known position from notifications captured during scan
    lp = record.get("last_position")
    if lp:
        lines += [
            "## Last known position (from device)",
            f"- lat={lp.get('latitude')}  lon={lp.get('longitude')}  alt={lp.get('altitude')}",
            f"- timestamp={_format_unix_ts(lp.get('timestamp'))}",
            "",
        ]

    # All other settings, sorted by id
    lines += ["## All settings", "", "| ID | Name | Decoded | Raw (hex) |",
              "|---:|------|---------|-----------|"]
    for sid in sorted(record["settings_raw"].keys()):
        meta = record["schema"].get(sid, {})
        name = meta.get("name", f"unknown_0x{sid:02X}")
        decoded_v = record["settings_decoded"].get(name)
        raw_hex = record["settings_raw"][sid]
        lines.append(f"| 0x{sid:02X} | `{name}` | {decoded_v} | `{raw_hex}` |")
    lines.append("")
    return "\n".join(lines)


async def run_report(args: argparse.Namespace) -> int:
    """
    Read-only mode. Walk every IRNAS device in BLE range, read its full
    settings dump, query ChirpStack for activation, and write per-device
    Markdown + a combined JSONL log. NO writes, NO reboots.
    """
    schema = load_settings_schema()
    schema_msg = (f"loaded {len(schema)} settings from schema"
                  if schema else "no schema file found -- report will use raw IDs")
    print(f"[report] schema: {schema_msg}")

    lns_cfg: Optional[dict] = getattr(args, "_lns_cfg", None)
    pre_run_addrs: dict[str, Optional[str]] = {}
    app_devices: set[str] = set()
    if lns_cfg and not args.no_verify:
        try:
            print(f"[report] querying ChirpStack at {lns_cfg['base_url']}...")
            app_euis = await list_app_dev_euis(lns_cfg)
            app_devices = {e.upper() for e in app_euis}
            pre_run_addrs = await snapshot_activations(lns_cfg, app_euis) if app_euis else {}
            print(f"[report] {len(app_devices)} devices in app, "
                  f"{sum(1 for v in pre_run_addrs.values() if v)} activated")
        except Exception as e:
            print(f"[report] [WARN] LNS query failed: {type(e).__name__}: {e}")

    reports_dir = SCRIPT_DIR / "reports"
    reports_dir.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    jsonl_path = reports_dir / f"device-reports-{timestamp}.jsonl"
    print(f"[report] writing per-device Markdown to {reports_dir}/")
    print(f"[report] writing combined JSONL to {jsonl_path}")
    print()

    sp_filter: Optional[set[str]] = set(args.sp) if args.sp else None
    seen_this_run: set[str] = set()
    written = 0
    failed = 0

    stopping = False

    def _handle_sig(*_):
        nonlocal stopping
        stopping = True
        print("\n[INTERRUPT] Finishing current device then exiting...")

    signal.signal(signal.SIGINT, _handle_sig)

    empty_windows = 0
    while not stopping:
        found, stats = await scan_for_irnas(seen_this_run, sp_filter, args.scan_seconds)
        print(f"[scan] {stats['irnas_seen']} IRNAS adverts in range, "
              f"{stats['matched']} new")
        if not found:
            empty_windows += 1
            if empty_windows >= args.idle_windows:
                break
            continue
        empty_windows = 0

        for device, adv_name in found:
            if stopping:
                break
            print(f"\n--> {adv_name} @ {device.address}")
            try:
                async with asyncio.timeout(float(args.per_device_timeout)):
                    async with DeviceSession(device) as session:
                        dev_eui = await session.read_dev_eui()
                        if not dev_eui:
                            print(f"  [FAIL] could not read DevEUI")
                            failed += 1
                            continue
                        print(f"  DevEUI: {dev_eui}")
                        raw_settings = await session.read_all_settings()
                        print(f"  Read {len(raw_settings)} settings")

                        # Decode known settings by name
                        decoded_by_name: dict[str, object] = {}
                        raw_by_id_hex: dict[int, str] = {}
                        for sid, raw in raw_settings.items():
                            meta = schema.get(sid, {})
                            name = meta.get("name", f"unknown_0x{sid:02X}")
                            decoded_by_name[name] = decode_setting_value(
                                name, raw, meta.get("conversion"))
                            raw_by_id_hex[sid] = raw.hex().upper()

                        # Last-known position is broadcast as an unsolicited
                        # notification on connect; pull it from the rx buffer.
                        last_position = _extract_last_position(session.rx_buffer)

                        cs_status = {"queried": bool(lns_cfg and not args.no_verify),
                                     "registered": dev_eui.upper() in app_devices,
                                     "devaddr": pre_run_addrs.get(dev_eui.upper()),
                                     "fcnt_up": None}

                        record = {
                            "dev_eui": dev_eui,
                            "ble_name": adv_name,
                            "ble_address": device.address,
                            "host": _short_hostname(),
                            "generated_at": datetime.now(timezone.utc).isoformat(),
                            "schema_loaded": bool(schema),
                            "settings_raw": raw_by_id_hex,
                            "settings_decoded": decoded_by_name,
                            "schema": schema,
                            "last_position": last_position,
                            "chirpstack": cs_status,
                        }

                        # Write per-device Markdown
                        sp_safe = "".join(c for c in (adv_name or "device") if c.isalnum())
                        md_path = reports_dir / f"{sp_safe}_{dev_eui}_{timestamp}.md"
                        md_path.write_text(_render_device_report_md(record))
                        print(f"  Wrote {md_path.name}")

                        # Append to JSONL (without the schema, to keep lines small)
                        jsonl_record = {k: v for k, v in record.items() if k != "schema"}
                        with jsonl_path.open("a") as f:
                            f.write(json.dumps(jsonl_record, default=str) + "\n")
                        written += 1
            except asyncio.TimeoutError:
                print(f"  [FAIL] per-device timeout after {args.per_device_timeout}s")
                failed += 1
            except Exception as e:
                print(f"  [FAIL] {type(e).__name__}: {e}")
                failed += 1
            await asyncio.sleep(INTER_DEVICE_PAUSE_SEC)

    print("\n" + "=" * 48)
    print("Report Summary")
    print("=" * 48)
    print(f"  Devices reported: {written}")
    print(f"  Failed:           {failed}")
    print(f"  Markdown:         {reports_dir}/")
    print(f"  JSONL:            {jsonl_path}")
    return 0 if failed == 0 else 1


def _short_hostname() -> str:
    try:
        import socket
        return socket.gethostname()
    except Exception:
        return "unknown"


def _extract_last_position(rx_buffer: list[bytes]) -> Optional[dict]:
    """
    The IRNAS firmware pushes a 'last position' record on the 0x1F port shortly
    after connect: [0x1F, 0xFE, 0x10, lat(4), lon(4), alt(4), ts(4)] (signed int32 each).
    """
    for frame in rx_buffer:
        if len(frame) >= 19 and frame[0] == 0x1F and frame[1] == 0xFE and frame[2] == 0x10:
            try:
                lat = int.from_bytes(frame[3:7], "little", signed=True) / COORD_SCALE
                lon = int.from_bytes(frame[7:11], "little", signed=True) / COORD_SCALE
                alt = int.from_bytes(frame[11:15], "little", signed=True)
                ts = int.from_bytes(frame[15:19], "little", signed=False)
                return {"latitude": lat, "longitude": lon, "altitude": alt, "timestamp": ts}
            except Exception:
                return None
    return None


# ---------------------------------------------------------------------------
# Wizard routing
# ---------------------------------------------------------------------------


def _is_wizard_invocation(args: argparse.Namespace) -> bool:
    """True if the user ran the script bare (no relevant flags) -> launch wizard."""
    if args.list_mode or args.by_address or args.report:
        return False
    if args.sp or args.dev_eui:
        return False
    if args.region is not None or args.profile is not None:
        return False
    if args.dry_run or args.force:
        return False
    if args.home_lat is not None or args.home_lon is not None:
        return False
    if not sys.stdin.isatty():
        return False
    if questionary is None:
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batch onboard IRNAS Smart Parks trackers over BLE. "
                    "Run with no args for the interactive wizard."
    )
    parser.add_argument("--list", action="store_true", dest="list_mode",
                        help="Diagnostic: scan and print every IRNAS advertisement, then exit.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scan, connect, read DevEUI + region, but do not write or reboot.")
    parser.add_argument("--idle-windows", type=int, default=3,
                        help="Exit after this many consecutive empty scan windows (default: 3).")
    parser.add_argument("--scan-seconds", type=float, default=DEFAULT_SCAN_WINDOW_SEC,
                        help=f"Scan window seconds (default: {DEFAULT_SCAN_WINDOW_SEC:g}).")
    parser.add_argument("--per-device-timeout", type=float, default=PER_DEVICE_TIMEOUT_SEC,
                        help=f"Hard cap on per-device BLE work in seconds "
                             f"(default: {PER_DEVICE_TIMEOUT_SEC:g}). Raise for slow devices.")
    parser.add_argument("--by-address", metavar="HEX",
                        help="Direct-connect to one specific peripheral by CoreBluetooth UUID / BT MAC.")
    parser.add_argument("--sp", nargs="+", type=normalize_sp, metavar="SP#",
                        help="One or more SP numbers (e.g. SP100280). Pre-connect filter on adv name.")
    parser.add_argument("--dev-eui", nargs="+", type=normalize_dev_eui, metavar="HEX",
                        help="Optional post-connect DevEUI validation (16 hex chars).")
    parser.add_argument("--force", action="store_true",
                        help="Bypass BOTH skips: re-onboard devices that are already "
                             "activated on ChirpStack AND devices already in the local ledger.")
    parser.add_argument("--region", type=parse_region_arg, default=None, metavar="N",
                        help="LoRaWAN region (1-13 Semtech enum, or name like US915). "
                             "If omitted: keep each device's current region.")
    parser.add_argument("--profile", choices=[p["id"] for p in UPLINK_PROFILES], default=None,
                        help="Apply a quick profile by id.")
    parser.add_argument("--no-set-time", action="store_true",
                        help="Don't write the device's init_time. Default is to set it to "
                             "the host's current UTC time on every visit.")
    parser.add_argument("--home-lat", type=float, default=None, metavar="DEG",
                        help="Set gps_init_lat (decimal degrees) as a GPS first-fix hint. "
                             "Use with --home-lon. Skipped if either is omitted.")
    parser.add_argument("--home-lon", type=float, default=None, metavar="DEG",
                        help="Set gps_init_lon (decimal degrees). Use with --home-lat.")
    parser.add_argument("--report", action="store_true",
                        help="Read-only mode: connect to every IRNAS device in range, dump "
                             "all settings + ChirpStack activation status to ./reports/, do not "
                             "write or reboot. Useful for QA snapshots and audits.")

    post_group = parser.add_mutually_exclusive_group()
    post_group.add_argument("--no-rejoin", dest="post_action", action="store_const", const="none",
                            help="Skip the post-write action entirely.")
    post_group.add_argument("--rejoin", dest="post_action", action="store_const", const="rejoin",
                            help="DIAGNOSTIC ONLY. Send cmd_join (0xA0) instead of cmd_reset. "
                                 "The IRNAS firmware silently drops cmd_join when its in-RAM "
                                 "lr_joined flag is true (which it usually is, even when the "
                                 "network has never seen a JoinRequest). The default --reboot is "
                                 "almost always what you want.")
    post_group.add_argument("--reboot", dest="post_action", action="store_const", const="reboot",
                            help="Send cmd_reset (0xA1). This is the default.")
    parser.set_defaults(post_action="reboot")

    parser.add_argument("--lns-url", default=None, help="Override CHIRPSTACK_BASE_URL.")
    parser.add_argument("--lns-api-key", default=None, help="Override CHIRPSTACK_API_KEY.")
    parser.add_argument("--lns-app-id", default=None, help="Override CHIRPSTACK_APPLICATION_ID.")
    parser.add_argument("--lns-env", default=None,
                        help=f"Path to .env file with CHIRPSTACK_* vars "
                             f"(default: {LOCAL_ENV_PATH}; if missing, the script "
                             f"will prompt and offer to save).")
    parser.add_argument("--verify-wait-min", type=int, default=DEFAULT_VERIFY_WAIT_MIN,
                        help=f"Minutes to wait after the last reboot before checking ChirpStack "
                             f"(default: {DEFAULT_VERIFY_WAIT_MIN}).")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip post-flight ChirpStack verification.")

    args = parser.parse_args()

    # Wire region/profile from flags into the same fields the wizard sets
    args.target_region = args.region
    args.target_profile = PROFILES_BY_ID.get(args.profile) if args.profile else None

    # Resolve LNS config up-front for non-wizard invocations too
    if not args.no_verify and not args.list_mode:
        args._lns_cfg = load_lns_config(args)
    else:
        args._lns_cfg = None

    try:
        if args.list_mode:
            return asyncio.run(list_mode(args))
        if args.report:
            return asyncio.run(run_report(args))
        if _is_wizard_invocation(args):
            return asyncio.run(run_wizard(args))
        if args.by_address:
            return asyncio.run(run_by_address(args))
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
