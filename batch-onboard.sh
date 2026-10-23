#!/usr/bin/env bash
#
# Wrapper for batch-onboard-us915.py:
#   - verifies python3 is present
#   - creates/activates a local .venv (via _setup-venv.sh)
#   - installs/refreshes requirements idempotently
#   - execs the Python script with all args forwarded
#
# Usage:
#   ./batch-onboard.sh                 # interactive wizard
#   ./batch-onboard.sh --list          # diagnostic, no connects
#   ./batch-onboard.sh --sp SP100219 --region US915 --profile transit
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PY_SCRIPT="${SCRIPT_DIR}/batch-onboard-us915.py"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_setup-venv.sh"

# macOS Bluetooth permission hint (one-time, harmless on Linux)
if [ "$(uname -s)" = "Darwin" ] && [ ! -f "${SCRIPT_DIR}/.venv/.bluetooth-hint-shown" ]; then
  cat <<'EOF'

[note] First run on macOS may prompt for Bluetooth permission. If the script
       hangs at "Scanning..." for more than ~30s with no devices found, open
       System Settings -> Privacy & Security -> Bluetooth and grant access
       to your terminal app (Terminal, iTerm, Cursor, etc.), then re-run.

EOF
  touch "${SCRIPT_DIR}/.venv/.bluetooth-hint-shown"
fi

# Prevent macOS sleep during the run.
#   -d  prevent display sleep  (critical: macOS throttles BT when display sleeps)
#   -i  prevent idle sleep
#   -s  prevent system sleep on AC power
# On Linux this is a no-op.
if [ "$(uname -s)" = "Darwin" ] && command -v caffeinate >/dev/null 2>&1; then
  exec caffeinate -dis "${VENV_PY}" "${PY_SCRIPT}" "$@"
else
  exec "${VENV_PY}" "${PY_SCRIPT}" "$@"
fi
