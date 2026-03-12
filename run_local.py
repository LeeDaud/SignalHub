from __future__ import annotations

import os
import threading
import webbrowser

import uvicorn


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def main() -> None:
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    reload_enabled = _to_bool(os.getenv("RELOAD"), False)
    auto_open = _to_bool(os.getenv("AUTO_OPEN_DASHBOARD"), True)
    dashboard_url = f"http://{host}:{port}/dashboard"

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
