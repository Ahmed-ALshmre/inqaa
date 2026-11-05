#!/usr/bin/env bash
# Dependency setup only. Secrets belong in Cloud's secret manager, never here.
set -euo pipefail
python -m pip install -r requirements.txt python-dotenv
python -X utf8 tools/check_cloud_handoff.py
