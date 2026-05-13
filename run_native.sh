#!/usr/bin/env bash
# Local dev entry point — same shape as other audimo-* addons.
# Activates the addon's virtualenv (creating it on first run) and
# starts the FastAPI sidecar on AUDIMO_ADDON_PORT (default 9010).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
  ./.venv/bin/pip install --upgrade pip
  ./.venv/bin/pip install -r requirements.txt
fi

export AUDIMO_ADDON_PORT="${AUDIMO_ADDON_PORT:-9010}"
exec ./.venv/bin/python server.py
