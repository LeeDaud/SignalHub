#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/signalhub}"
SERVICE_NAME="signalhub"
DOMAIN="signal.licheng.website"

if [[ ! -d "$APP_DIR" ]]; then
  echo "App directory not found: $APP_DIR" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y \
  python3 \
  python3-venv \
  python3-pip \
  nginx \
  certbot \
  python3-certbot-nginx \
  ca-certificates \
  curl

if ! id -u signalhub >/dev/null 2>&1; then
  useradd --system --home "$APP_DIR" --shell /usr/sbin/nologin signalhub
fi

mkdir -p "$APP_DIR/exports" "$APP_DIR/logs"
chown -R signalhub:signalhub "$APP_DIR"

if [[ ! -f "$APP_DIR/.env" ]]; then
  cp "$APP_DIR/deploy/env/signalhub.env.example" "$APP_DIR/.env"
  chown signalhub:signalhub "$APP_DIR/.env"
  echo "Created $APP_DIR/.env from template. Edit it before production use."
fi

python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --upgrade pip wheel
"$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"

cp "$APP_DIR/deploy/systemd/signalhub.service" "/etc/systemd/system/${SERVICE_NAME}.service"
cp "$APP_DIR/deploy/nginx/${DOMAIN}.conf" "/etc/nginx/sites-available/${DOMAIN}.conf"
ln -sf "/etc/nginx/sites-available/${DOMAIN}.conf" "/etc/nginx/sites-enabled/${DOMAIN}.conf"
rm -f /etc/nginx/sites-enabled/default

nginx -t
systemctl daemon-reload
systemctl enable nginx
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
systemctl restart nginx

echo
echo "SignalHub service installed."
echo "Next step for HTTPS:"
echo "  certbot --nginx -d ${DOMAIN}"
echo
echo "Health checks:"
echo "  systemctl status ${SERVICE_NAME}"
echo "  curl http://127.0.0.1:8000/healthz"
