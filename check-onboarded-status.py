#!/usr/bin/env python3
"""
Query ChirpStack for every DevEUI in onboarded.json and report join status.

Non-destructive: only GET requests. No writes.

A device is considered JOINED iff its activation endpoint returns a non-empty
devAddr. lastSeenAt is intentionally NOT used because in ChirpStack v4 it only
updates on data uplinks, which can lag a successful join by hours.

Credential discovery (in order):
  1. CLI flags (--lns-url, --lns-api-key, --lns-app-id)
  2. Local .env file next to this script (or --lns-env PATH)
  3. Process environment variables (CHIRPSTACK_BASE_URL etc.)

Usage:
    python3 check-onboarded-status.py
    python3 check-onboarded-status.py --only-silent    # devices that never joined
    python3 check-onboarded-status.py --only-joined    # devices that have joined
    python3 check-onboarded-status.py --lns-env /path/to/.env
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

SCRIPT_DIR = Path(__file__).resolve().parent
LEDGER_PATH = SCRIPT_DIR / "onboarded.json"
LOCAL_ENV_PATH = SCRIPT_DIR / ".env"


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


def resolve_lns_config(args: argparse.Namespace) -> tuple[Optional[str], Optional[str]]:
    """Return (base_url, api_key) or exits with a helpful message."""
    env_path = Path(args.lns_env) if args.lns_env else LOCAL_ENV_PATH
    env = load_env_file(env_path) if env_path.exists() else {}

    base_url = (
        args.lns_url
        or env.get("CHIRPSTACK_BASE_URL")
        or os.environ.get("CHIRPSTACK_BASE_URL")
    )
    api_key = (
        args.lns_api_key
        or env.get("CHIRPSTACK_API_KEY")
        or os.environ.get("CHIRPSTACK_API_KEY")
    )

    if not base_url or not api_key:
        sys.exit(
            "ChirpStack credentials not found.\n"
            f"Looked in: {env_path}, env vars CHIRPSTACK_BASE_URL/API_KEY, CLI flags.\n"
            "Either run ./batch-onboard.sh once (the wizard will save credentials to .env),\n"
            "or copy .env.example to .env and fill it in,\n"
            "or pass --lns-url and --lns-api-key on the command line."
        )
    return base_url, api_key


def fetch_activation(base_url: str, api_key: str, dev_eui: str) -> dict:
    """Returns {devaddr: str|None, fcnt_up: int|None, error: str|None}."""
    url = f"{base_url.rstrip('/')}/api/devices/{dev_eui.lower()}/activation"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"devaddr": None, "fcnt_up": None, "error": None}
        return {"devaddr": None, "fcnt_up": None, "error": f"HTTP {e.code}"}
    except Exception as e:
        return {"devaddr": None, "fcnt_up": None, "error": f"{type(e).__name__}: {e}"}

    activation = data.get("deviceActivation")
    if not activation:
        return {"devaddr": None, "fcnt_up": None, "error": None}
    return {
        "devaddr": activation.get("devAddr") or None,
        "fcnt_up": activation.get("fCntUp"),
        "error": None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only-silent", action="store_true",
                        help="Only list devices that never joined.")
    parser.add_argument("--only-joined", action="store_true",
                        help="Only list devices that have joined.")
    parser.add_argument("--lns-url", default=None, help="Override CHIRPSTACK_BASE_URL.")
    parser.add_argument("--lns-api-key", default=None, help="Override CHIRPSTACK_API_KEY.")
    parser.add_argument("--lns-app-id", default=None,
                        help="(Reserved; not currently used by this script.)")
    parser.add_argument("--lns-env", default=None,
                        help=f"Path to .env file (default: {LOCAL_ENV_PATH}).")
    args = parser.parse_args()

    base_url, api_key = resolve_lns_config(args)

    if not LEDGER_PATH.exists():
        sys.exit(f"Ledger not found: {LEDGER_PATH}\n"
                 "Run ./batch-onboard.sh first to onboard some devices.")
    ledger = json.loads(LEDGER_PATH.read_text())
    devices = ledger.get("devices", {})
    if not devices:
        print("Ledger is empty. Nothing to check.")
        return 0
    print(f"Checking activation for {len(devices)} devices against {base_url}...\n")

    results = []  # (sp_name, dev_eui, status, devaddr_or_error, fcnt_up)
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {
            pool.submit(fetch_activation, base_url, api_key, eui): (eui, entry)
            for eui, entry in devices.items()
        }
        for future in as_completed(futures):
            eui, entry = futures[future]
            sp_name = entry.get("ble_name", "?")
            r = future.result()
            if r["error"]:
                results.append((sp_name, eui, "ERROR", r["error"], None))
            elif r["devaddr"]:
                results.append((sp_name, eui, "JOINED", r["devaddr"], r["fcnt_up"]))
            else:
                results.append((sp_name, eui, "NEVER JOINED", "-", None))

    results.sort()
    joined = [r for r in results if r[2] == "JOINED"]
    silent = [r for r in results if r[2] == "NEVER JOINED"]
    errors = [r for r in results if r[2] == "ERROR"]

    def show(rows):
        for sp, eui, status, detail, fcnt in rows:
            fcnt_str = f"  fCntUp={fcnt}" if fcnt is not None else ""
            print(f"  {sp:<10} {eui}  {status:<13} devAddr={detail}{fcnt_str}")

    if args.only_silent:
        print(f"Devices that never joined ({len(silent)}/{len(results)}):")
        show(silent)
    elif args.only_joined:
        print(f"Devices that have joined ({len(joined)}/{len(results)}):")
        show(joined)
    else:
        print(f"JOINED ({len(joined)}):")
        show(joined)
        print(f"\nNEVER JOINED ({len(silent)}):")
        show(silent)
        if errors:
            print(f"\nERRORS ({len(errors)}):")
            show(errors)

    print(f"\nSummary: {len(joined)} joined, {len(silent)} silent, "
          f"{len(errors)} errors, {len(results)} total")
    return 0


if __name__ == "__main__":
    sys.exit(main())
