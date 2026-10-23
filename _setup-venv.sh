#!/usr/bin/env bash
#
# _setup-venv.sh -- shared venv bootstrap, sourced by the user-facing wrappers.
#
# Sourcing this script:
#   - verifies python3 >= 3.10 is on PATH
#   - creates ${SCRIPT_DIR}/.venv on first use
#   - rebuilds .venv if it was relocated (activate scripts hard-code an
#     absolute path; moving/renaming the parent dir silently breaks the venv
#     and `python3` falls back to a system interpreter without our deps)
#   - installs/updates ${SCRIPT_DIR}/requirements.txt (idempotent: only re-runs
#     pip when requirements.txt is newer than the install stamp)
#   - exports VENV_PY pointing at the venv's python3 so callers can exec it
#     directly without depending on PATH/activation
#
# Caller contract:
#   - must `set -euo pipefail` before sourcing
#   - must export SCRIPT_DIR pointing at the directory that owns .venv and
#     requirements.txt (typically the wrapper's own directory)
#

if [ -z "${SCRIPT_DIR:-}" ]; then
  echo "ERROR: _setup-venv.sh sourced without SCRIPT_DIR set." >&2
  exit 1
fi

VENV_DIR="${SCRIPT_DIR}/.venv"
VENV_PY="${VENV_DIR}/bin/python3"
REQ_FILE="${SCRIPT_DIR}/requirements.txt"
REQ_STAMP="${VENV_DIR}/.requirements-stamp"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is not on PATH. Install Python 3.10+ and re-run." >&2
  exit 1
fi

PY_VERSION="$(python3 -c 'import sys; print("{}.{}".format(sys.version_info[0], sys.version_info[1]))')"
PY_MAJOR="${PY_VERSION%%.*}"
PY_MINOR="${PY_VERSION##*.}"
if [ "${PY_MAJOR}" -lt 3 ] || { [ "${PY_MAJOR}" -eq 3 ] && [ "${PY_MINOR}" -lt 10 ]; }; then
  echo "ERROR: Python ${PY_VERSION} detected. Need Python 3.10 or newer." >&2
  exit 1
fi

# Detect a stale/relocated venv. The activate script and the python3 shebang
# both hard-code an absolute path at creation time. If the parent directory
# was moved or renamed, the venv's python3 either won't exist at all or will
# fail to start because its embedded sys.prefix doesn't match its real path.
venv_is_healthy() {
  [ -x "${VENV_PY}" ] || return 1
  # Sanity: does the embedded sys.prefix match where the venv actually lives?
  local actual
  actual="$("${VENV_PY}" -c 'import sys; print(sys.prefix)' 2>/dev/null || true)"
  [ "${actual}" = "${VENV_DIR}" ]
}

if [ -d "${VENV_DIR}" ] && ! venv_is_healthy; then
  echo "Detected stale/relocated .venv at ${VENV_DIR}; rebuilding..."
  rm -rf "${VENV_DIR}"
fi

if [ ! -d "${VENV_DIR}" ]; then
  echo "Creating virtualenv at ${VENV_DIR}..."
  python3 -m venv "${VENV_DIR}"
fi

# Activation is cosmetic (sets PS1 / VIRTUAL_ENV / PATH); we use VENV_PY
# directly for correctness. Sourcing it anyway so the Python guard's
# `sys.prefix != sys.base_prefix` check also passes if a script is invoked
# via PATH-resolved `python3` further down the chain.
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

if [ ! -f "${REQ_FILE}" ]; then
  echo "ERROR: requirements file not found at ${REQ_FILE}" >&2
  exit 1
fi

if [ ! -f "${REQ_STAMP}" ] || [ "${REQ_FILE}" -nt "${REQ_STAMP}" ]; then
  echo "Installing/updating requirements from ${REQ_FILE}..."
  "${VENV_PY}" -m pip install --quiet --upgrade pip
  "${VENV_PY}" -m pip install --quiet -r "${REQ_FILE}"
  touch "${REQ_STAMP}"
fi

# Tell the Python guard (see top of each .py) that the wrapper bootstrap ran.
export BUOY_VENV_BOOTSTRAPPED=1
export VENV_PY
