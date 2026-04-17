#!/usr/bin/env bash
#
# Wrapper for batch-onboard-us915.py:
#   - verifies python3 is present
#   - creates/activates a local .venv
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

VENV_DIR="${SCRIPT_DIR}/.venv"
REQ_FILE="${SCRIPT_DIR}/requirements.txt"
REQ_STAMP="${VENV_DIR}/.requirements-stamp"
PY_SCRIPT="${SCRIPT_DIR}/batch-onboard-us915.py"

# 1. python3 present?
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is not on PATH. Install Python 3.10+ and re-run."
  exit 1
fi

PY_VERSION="$(python3 -c 'import sys; print("{}.{}".format(sys.version_info[0], sys.version_info[1]))')"
PY_MAJOR="${PY_VERSION%%.*}"
PY_MINOR="${PY_VERSION##*.}"
if [ "${PY_MAJOR}" -lt 3 ] || { [ "${PY_MAJOR}" -eq 3 ] && [ "${PY_MINOR}" -lt 10 ]; }; then
  echo "ERROR: Python ${PY_VERSION} detected. Need Python 3.10 or newer."
  exit 1
fi

# 2. venv
if [ ! -d "${VENV_DIR}" ]; then
  echo "Creating virtualenv at ${VENV_DIR}..."
  python3 -m venv "${VENV_DIR}"
fi

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

# 3. install requirements (only if requirements.txt is newer than the stamp)
if [ ! -f "${REQ_STAMP}" ] || [ "${REQ_FILE}" -nt "${REQ_STAMP}" ]; then
  echo "Installing/updating requirements from ${REQ_FILE}..."
  python3 -m pip install --quiet --upgrade pip
  python3 -m pip install --quiet -r "${REQ_FILE}"
  touch "${REQ_STAMP}"
fi

# 4. macOS Bluetooth permission hint (one-time, harmless on Linux)
if [ "$(uname -s)" = "Darwin" ] && [ ! -f "${VENV_DIR}/.bluetooth-hint-shown" ]; then
  cat <<'EOF'

[note] First run on macOS may prompt for Bluetooth permission. If the script
       hangs at "Scanning..." for more than ~30s with no devices found, open
       System Settings -> Privacy & Security -> Bluetooth and grant access
       to your terminal app (Terminal, iTerm, Cursor, etc.), then re-run.

EOF
  touch "${VENV_DIR}/.bluetooth-hint-shown"
fi

# 5. Prevent macOS idle sleep during the run (caffeinate -i keeps the CPU/
#    process alive without requiring AC power; display may still dim/sleep).
#    On Linux this is a no-op. The -i flag prevents idle sleep only; use -s
#    to also prevent sleep on battery if you need that.
if [ "$(uname -s)" = "Darwin" ] && command -v caffeinate >/dev/null 2>&1; then
  exec caffeinate -i python3 "${PY_SCRIPT}" "$@"
else
  exec python3 "${PY_SCRIPT}" "$@"
fi
