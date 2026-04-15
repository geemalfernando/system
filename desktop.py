import os
import socket
import threading
from pathlib import Path


def _pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _default_data_dir() -> str:
    return str(Path.home() / ".lion_car_sale")


def main():
    os.environ.setdefault("STORAGE_BACKEND", "local")
    os.environ.setdefault("APP_DATA_DIR", _default_data_dir())

    data_dir = Path(os.environ["APP_DATA_DIR"]).expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("UPLOAD_FOLDER", str(data_dir / "uploads"))

    port = _pick_port()

    from server import app

    def run_server():
        app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False)

    t = threading.Thread(target=run_server, daemon=True)
    t.start()

    try:
        import webview
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency: pywebview\n\n"
            "Install it, then run again:\n"
            "  python -m pip install -r requirements-desktop.txt\n"
            "  python desktop.py\n"
        ) from exc

    webview.create_window("Lion Car Sale", f"http://127.0.0.1:{port}/", min_size=(1100, 700))
    webview.start()


if __name__ == "__main__":
    main()

