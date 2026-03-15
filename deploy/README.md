# SignalHub Server Deployment

These files are prepared for Ubuntu + `systemd + nginx` deployment with:

- app directory: `/opt/signalhub`
- service name: `signalhub`
- domain: `signal.licheng.website`
- upstream bind: `127.0.0.1:8000`

## Files

- `deploy/env/signalhub.env.example`
  Server-side environment template.

- `deploy/systemd/signalhub.service`
  `systemd` unit file for the FastAPI app.

- `deploy/nginx/signal.licheng.website.conf`
  Nginx reverse proxy config for the subdomain.

- `deploy/scripts/install_ubuntu.sh`
  First-time bootstrap script for Ubuntu.

- `deploy/scripts/update_app.sh`
  Reinstall dependencies and restart the service after code updates.

## Expected server layout

```text
/opt/signalhub
├─ .env
├─ .venv/
├─ deploy/
├─ exports/
├─ logs/
├─ signalhub/
├─ requirements.txt
└─ signalhub.db
```

## First-time deployment

1. Upload the project to `/opt/signalhub`
2. Copy the env template:

```bash
cp /opt/signalhub/deploy/env/signalhub.env.example /opt/signalhub/.env
```

3. Edit `/opt/signalhub/.env`
4. Run:

```bash
bash /opt/signalhub/deploy/scripts/install_ubuntu.sh
```

5. Issue HTTPS:

```bash
certbot --nginx -d signal.licheng.website
```

## Update after code changes

```bash
bash /opt/signalhub/deploy/scripts/update_app.sh
```

## Useful checks

```bash
systemctl status signalhub
journalctl -u signalhub -n 100 --no-pager
nginx -t
curl http://127.0.0.1:8000/healthz
curl http://127.0.0.1:8000/system/status
```

## Notes

- The nginx site file is prepared to take over `signal.licheng.website`.
- If that subdomain is already serving another app, enabling this config will replace it.
- The service runs as a dedicated system user: `signalhub`.
