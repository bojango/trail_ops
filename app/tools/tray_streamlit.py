from __future__ import annotations

"""
TrailOps - System tray controller for Streamlit server

Features
- Runs Streamlit server in the background (no console if you launch the tray app via pythonw / packaged exe)
- Tray icon + menu:
  - Open dashboard
  - Start / Stop / Restart server
  - Open server log
  - Quit
- Toast notification if the Streamlit process exits unexpectedly

Run (dev):
    python -m app.tools.tray_streamlit

Run (windowless):
    C:\trail_ops\.venv\Scripts\pythonw.exe -m app.tools.tray_streamlit

Dependencies (install once):
    pip install pystray pillow win10toast
"""

import os
import subprocess
import threading
import time
import webbrowser
from pathlib import Path

import pystray
from PIL import Image, ImageDraw

try:
    from win10toast import ToastNotifier
except Exception:  # pragma: no cover
    ToastNotifier = None  # type: ignore


PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../trail_ops
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
SERVER_LOG = LOG_DIR / "streamlit_server.log"


def _icon_image(color: tuple[int, int, int]) -> Image.Image:
    # Simple mountain-ish icon
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.polygon([(8, 52), (26, 20), (44, 52)], fill=color)  # big peak
    d.polygon(
        [(22, 52), (34, 34), (52, 52)],
        fill=(min(color[0] + 30, 255), min(color[1] + 30, 255), min(color[2] + 30, 255)),
    )  # small peak
    d.rectangle([0, 52, 64, 64], fill=(30, 30, 30, 255))
    return img


class TrailOpsTray:
    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self._stop_monitor = threading.Event()
        self._toaster = ToastNotifier() if ToastNotifier else None

        # Start as "stopped" (red) until server starts
        self.icon = pystray.Icon(
            "TrailOps",
            _icon_image((180, 0, 0)),
            "TrailOps (stopped)",
            menu=pystray.Menu(
                pystray.MenuItem("Open dashboard", self.open_dashboard),
                pystray.MenuItem("Start server", self.start_server),
                pystray.MenuItem("Stop server", self.stop_server),
                pystray.MenuItem("Restart server", self.restart_server),
                pystray.MenuItem("Open server log", self.open_server_log),
                pystray.Menu.SEPARATOR,
                pystray.MenuItem("Quit", self.quit),
            ),
        )

    def notify(self, title: str, msg: str) -> None:
        if self._toaster:
            try:
                self._toaster.show_toast(title, msg, duration=6, threaded=True)
            except Exception:
                pass

    def open_dashboard(self, _icon=None, _item=None) -> None:
        webbrowser.open("http://localhost:8501")

    def open_server_log(self, _icon=None, _item=None) -> None:
        try:
            if SERVER_LOG.exists():
                os.startfile(str(SERVER_LOG))  # type: ignore[attr-defined]
            else:
                self.notify("TrailOps", "No server log yet.")
        except Exception:
            pass

    def _set_running(self) -> None:
        self.icon.icon = _icon_image((0, 180, 0))
        self.icon.title = "TrailOps (running)"

    def _set_stopped(self) -> None:
        self.icon.icon = _icon_image((180, 0, 0))
        self.icon.title = "TrailOps (stopped)"

    def _set_crashed(self) -> None:
        self.icon.icon = _icon_image((180, 0, 0))
        self.icon.title = "TrailOps (crashed)"

    def start_server(self, _icon=None, _item=None) -> None:
        if self.proc and self.proc.poll() is None:
            self.notify("TrailOps", "Server already running.")
            self._set_running()
            return

        venv_python = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
        if not venv_python.exists():
            self.notify("TrailOps", "venv python.exe not found. Check .venv.")
            return

        cmd = [
            str(venv_python),
            "-m",
            "streamlit",
            "run",
            "app/main_app.py",
            "--server.port=8501",
            "--server.headless=true",
            "--browser.gatherUsageStats=false",
        ]

        try:
            f = SERVER_LOG.open("a", encoding="utf-8")
            self.proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=f,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            self._set_running()
            self.notify("TrailOps", "Server started.")
        except Exception as e:
            self.notify("TrailOps", f"Failed to start server: {e!r}")
            self._set_stopped()

    def stop_server(self, _icon=None, _item=None) -> None:
        if not self.proc or self.proc.poll() is not None:
            self.notify("TrailOps", "Server not running.")
            self._set_stopped()
            return
        try:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except Exception:
                self.proc.kill()
            self.notify("TrailOps", "Server stopped.")
        except Exception:
            self.notify("TrailOps", "Failed to stop server.")
        finally:
            self.proc = None
            self._set_stopped()

    def restart_server(self, _icon=None, _item=None) -> None:
        # Stop then start, with a small delay
        try:
            self.stop_server()
            time.sleep(1.0)
        except Exception:
            pass
        self.start_server()

    def _monitor(self) -> None:
        last_pid: int | None = None
        while not self._stop_monitor.is_set():
            if self.proc and self.proc.poll() is not None:
                code = self.proc.returncode
                pid = self.proc.pid
                if last_pid != pid:
                    last_pid = pid
                    self.proc = None
                    self._set_crashed()
                    self.notify("TrailOps", f"Server exited (code {code}). Check logs.")
            time.sleep(1.0)

    def quit(self, _icon=None, _item=None) -> None:
        self._stop_monitor.set()
        try:
            self.stop_server()
        except Exception:
            pass
        self.icon.stop()

    def run(self) -> None:
        t = threading.Thread(target=self._monitor, daemon=True)
        t.start()
        # Start server automatically on tray start (comment out if you prefer manual)
        self.start_server()
        self.icon.run()


def main() -> None:
    TrailOpsTray().run()


if __name__ == "__main__":
    main()
