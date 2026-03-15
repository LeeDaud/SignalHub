from __future__ import annotations

import os
import socket
import threading
import urllib.error
import urllib.request
import webbrowser

import uvicorn


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _port_is_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.8)
        return sock.connect_ex((host, port)) == 0


def _is_signalhub_running(host: str, port: int) -> bool:
    for path in ("/healthz", "/system/status"):
        try:
            with urllib.request.urlopen(f"http://{host}:{port}{path}", timeout=2) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, TimeoutError, OSError):
            continue
    return False


def main() -> None:
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    reload_enabled = _to_bool(os.getenv("RELOAD"), False)
    auto_open = _to_bool(os.getenv("AUTO_OPEN_DASHBOARD"), True)
    dashboard_url = f"http://{host}:{port}/dashboard"

    if _port_is_open(host, port):
        if _is_signalhub_running(host, port):
            print(f"SignalHub is already running at http://{host}:{port}")
            print(f"Dashboard URL: {dashboard_url}")
            if auto_open:
                webbrowser.open(dashboard_url)
            return
        raise SystemExit(
            f"Port {port} is already in use by another process. "
            "Stop that process or set PORT to a different value before starting SignalHub."
        )

    if auto_open:
        threading.Timer(1.2, lambda: webbrowser.open(dashboard_url)).start()

    print(f"SignalHub server starting at http://{host}:{port}")
    print(f"Dashboard URL: {dashboard_url}")

    uvicorn.run(
        "signalhub.app.main:app",
        host=host,
        port=port,
        reload=reload_enabled,
    )


if __name__ == "__main__":
    main()
