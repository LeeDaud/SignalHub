#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/signalhub}"
SERVICE_NAME="signalhub"

if [[ ! -d "$APP_DIR" ]]; then
  echo "App directory not found: $APP_DIR" >&2
  exit 1
fi

"$APP_DIR/.venv/bin/pip" install --upgrade pip
"$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"

chown -R signalhub:signalhub "$APP_DIR"

systemctl daemon-reload
systemctl restart "$SERVICE_NAME"
systemctl reload nginx

echo "SignalHub updated and restarted."
