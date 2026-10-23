#!/usr/bin/env bash
#
# check-onboarded-status.sh -- self-bootstrapping wrapper for
# check-onboarded-status.py.
#
# Reads onboarded.json + .env (or CLI flags) and queries ChirpStack for
# every device's activation state. Read-only, no BLE. Safe to run anytime.
#
# Usage:
#   ./check-onboarded-status.sh                   # full report
#   ./check-onboarded-status.sh --only-silent     # devices that never joined
#   ./check-onboarded-status.sh --only-joined     # devices that joined
#   ./check-onboarded-status.sh --lns-env /path/to/.env
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PY_SCRIPT="${SCRIPT_DIR}/check-onboarded-status.py"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_setup-venv.sh"

exec "${VENV_PY}" "${PY_SCRIPT}" "$@"
