#!/usr/bin/env bash
#
# batch-locate.sh -- thin wrapper that runs batch-onboard.sh in --locate mode.
#
# Walks every IRNAS device in BLE range and tells each one to acquire a fresh
# GPS fix and uplink it via LoRaWAN. Useful for getting fresh pins on a map
# without waiting for the device's natural status_send_interval to elapse.
#
# READ-ONLY on settings: does NOT modify any persisted setting on the device
# (region, profile, init_time, GPS home, etc.). Only sends commands. Shares
# the ledger with batch-onboard.
#
# Usage:
#   ./batch-locate.sh                    # locate every IRNAS device in range
#   ./batch-locate.sh --sp SP100272      # locate one specific SP#
#   ./batch-locate.sh --also-send-status # also force a status uplink (immediate)
#   ./batch-locate.sh --no-verify        # skip the post-flight uplink check
#   ./batch-locate.sh --locate-wait-min 5  # wait 5 min before checking ChirpStack
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/batch-onboard.sh" --locate "$@"
