import json
import os
import queue
import re
import subprocess
import sys
import threading
from datetime import date
from pathlib import Path
from typing import Any, Mapping

import webview
import yaml

from .config_loader import load_config, normalize_config_paths
from .pipeline import run_pipeline


SETTINGS_PATH = Path.home() / ".h2h_iqx_pipeline_gui.json"
MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")


def _resource_path(relative: str) -> Path:
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parents[2]))
    return base / relative


def _load_yaml(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _tail_lines(path: Path, limit: int = 200) -> list[str]:
    try:
        with path.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > limit:
            lines = lines[-limit:]
        return [line.rstrip("\n") for line in lines]
    except FileNotFoundError:
        return []


def _open_path(path: Path) -> None:
    if sys.platform.startswith("win"):
        try:
            if hasattr(os, "startfile"):
                os.startfile(str(path))  # type: ignore[attr-defined]
                return
        except OSError:
            pass
        if path.is_file():
            subprocess.run(["notepad.exe", str(path)], check=False)
        else:
            subprocess.run(["explorer.exe", str(path)], check=False)
    elif sys.platform == "darwin":
        subprocess.run(["open", str(path)], check=False)
    else:
        subprocess.run(["xdg-open", str(path)], check=False)


def _first_selection(selection: Any) -> str | None:
    if not selection:
        return None
    if isinstance(selection, (list, tuple)):
        return str(selection[0]) if selection else None
    return str(selection)


def _resolve_config_path(value: str, base_dir: Path) -> str:
    candidate = Path(value)
    if candidate.is_absolute():
        return str(candidate)
    return str((base_dir / candidate).resolve())


class PipelineWebAPI:
    def __init__(self) -> None:
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self._running = False
        self._last_output_root: Path | None = None
        self._last_log_file: Path | None = None
        self._window: Any | None = None

    def bind_window(self, window: Any) -> None:
        self._window = window

    def get_state(self) -> dict[str, Any]:
        settings = self._load_settings()
        return {
            "config_path": settings.get("config_path", ""),
            "input_root": settings.get("input_root", ""),
            "output_root": settings.get("output_root", ""),
            "month": settings.get("month", date.today().strftime("%Y-%m")),
            "running": self._running,
            "status": "Idle",
            "output_ready": bool(self._last_output_root),
            "log_ready": bool(self._last_log_file),
            "last_output_root": str(self._last_output_root) if self._last_output_root else "",
            "last_log_file": str(self._last_log_file) if self._last_log_file else "",
        }

    def get_updates(self) -> list[dict[str, Any]]:
        updates: list[dict[str, Any]] = []
        while True:
            try:
                updates.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return updates

    def prefill_from_config(self, config_path: str) -> dict[str, str]:
        path = Path(config_path).expanduser() if config_path else None
        if not path or not path.exists():
            return {}
        try:
            cfg = _load_yaml(path)
            if not isinstance(cfg, Mapping):
                return {}
            normalize_config_paths(cfg, path)
        except Exception:
            return {}

        result: dict[str, str] = {}
        base_dir = path.parent
        paths = cfg.get("paths", {})
        if isinstance(paths, Mapping):
            if paths.get("input_root"):
                result["input_root"] = _resolve_config_path(str(paths["input_root"]), base_dir)
            if paths.get("output_root"):
                result["output_root"] = _resolve_config_path(str(paths["output_root"]), base_dir)
        run_cfg = cfg.get("run", {})
        if isinstance(run_cfg, Mapping) and run_cfg.get("current_month"):
            result["month"] = str(run_cfg["current_month"])
        return result

    def choose_config(self) -> str | None:
        if not self._window:
            return None
        file_types = [
            ("YAML files (*.yml;*.yaml)", "*.yml;*.yaml"),
            ("All files (*.*)", "*.*"),
        ]
        selection = self._window.create_file_dialog(
            webview.OPEN_DIALOG, allow_multiple=False, file_types=file_types
        )
        return _first_selection(selection)

    def choose_input_root(self) -> str | None:
        if not self._window:
            return None
        selection = self._window.create_file_dialog(webview.FOLDER_DIALOG)
        return _first_selection(selection)

    def choose_output_root(self) -> str | None:
        if not self._window:
            return None
        selection = self._window.create_file_dialog(webview.FOLDER_DIALOG)
        return _first_selection(selection)

    def save_settings(
        self, config_path: str, input_root: str, output_root: str, month: str
    ) -> None:
        settings = {
            "config_path": config_path.strip(),
            "input_root": input_root.strip(),
            "output_root": output_root.strip(),
            "month": month.strip(),
        }
        try:
            SETTINGS_PATH.write_text(json.dumps(settings, indent=2), encoding="utf-8")
        except Exception:
            pass

    def run_pipeline(
        self, config_path: str, input_root_raw: str, output_root_raw: str, month_raw: str
    ) -> dict[str, Any]:
        if self._running:
            return {"ok": False, "error": "Pipeline already running."}

        config_path = (config_path or "").strip()
        if not config_path:
            return {"ok": False, "error": "Please select a config file."}
        if not Path(config_path).exists():
            return {"ok": False, "error": f"Config file not found: {config_path}"}

        month = (month_raw or "").strip()
        if month and not MONTH_PATTERN.match(month):
            return {"ok": False, "error": "Month must be in YYYY-MM format."}

        self._set_running(True)
        self._queue.put({"type": "log_clear"})
        self._enqueue_log("Starting pipeline run...")
        self.save_settings(config_path, input_root_raw, output_root_raw, month_raw)

        thread = threading.Thread(
            target=self._run_pipeline_worker,
            args=(config_path, input_root_raw, output_root_raw, month_raw),
            daemon=True,
        )
        thread.start()
        return {"ok": True}

    def open_output(self) -> dict[str, Any]:
        if not self._last_output_root or not self._last_output_root.exists():
            return {"ok": False, "error": "No output folder available yet."}
        _open_path(self._last_output_root)
        return {"ok": True}

    def open_log(self) -> dict[str, Any]:
        if not self._last_log_file or not self._last_log_file.exists():
            return {"ok": False, "error": "No log file available yet."}
        _open_path(self._last_log_file)
        return {"ok": True}

    def _run_pipeline_worker(
        self, config_path: str, input_root_raw: str, output_root_raw: str, month_raw: str
    ) -> None:
        try:
            overrides: dict[str, dict[str, str]] = {"paths": {}}
            input_root = Path(input_root_raw) if input_root_raw else None
            output_root = Path(output_root_raw) if output_root_raw else None

            if input_root:
                overrides["paths"]["input_root"] = str(input_root)
            if output_root:
                overrides["paths"]["output_root"] = str(output_root)

            cfg = load_config(
                Path(config_path),
                overrides=overrides if overrides["paths"] else None,
            )

            if input_root is None:
                input_root = Path(cfg["paths"]["input_root"])
            if output_root is None:
                output_root = input_root / "output"

            paths_cfg = cfg.setdefault("paths", {})
            paths_cfg["output_root"] = str(output_root)
            paths_cfg["log_dir"] = str(output_root / "logs")

            month = month_raw or cfg.get("run", {}).get("current_month") or date.today().strftime("%Y-%m")
            if not MONTH_PATTERN.match(month):
                raise ValueError("Month must be in YYYY-MM format.")

            self._enqueue_status("Running pipeline...")
            self._enqueue_log(f"Config: {config_path}")
            self._enqueue_log(f"Input root: {input_root}")
            self._enqueue_log(f"Output root: {output_root}")
            self._enqueue_log(f"Month: {month}")

            run_pipeline(month=month, input_root=input_root, config=cfg)

            log_file = Path(paths_cfg["log_dir"]) / "pipeline.log"
            log_lines = _tail_lines(log_file)
            self._enqueue_log("Pipeline run completed.")
            if log_lines:
                self._enqueue_log("---- Pipeline log (tail) ----")
                for line in log_lines:
                    self._enqueue_log(line)

            self._last_output_root = output_root
            self._last_log_file = log_file
            self._enqueue_status("Run complete")
        except Exception as exc:
            self._enqueue_error(str(exc))
        finally:
            self._set_running(False)

    def _emit_state(self) -> None:
        self._queue.put(
            {
                "type": "state",
                "running": self._running,
                "output_root": str(self._last_output_root) if self._last_output_root else "",
                "log_file": str(self._last_log_file) if self._last_log_file else "",
            }
        )

    def _enqueue_log(self, message: str) -> None:
        self._queue.put({"type": "log", "message": message})

    def _enqueue_status(self, message: str) -> None:
        self._queue.put({"type": "status", "message": message})

    def _enqueue_error(self, message: str) -> None:
        self._queue.put({"type": "error", "message": message})
        self._queue.put({"type": "log", "message": f"ERROR: {message}"})

    def _set_running(self, running: bool) -> None:
        self._running = running
        self._emit_state()

    def _load_settings(self) -> dict[str, str]:
        if SETTINGS_PATH.exists():
            try:
                return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}


def main() -> None:
    webview.settings["ALLOW_FILE_URLS"] = True
    api = PipelineWebAPI()
    index_path = _resource_path("web/index.html").resolve()
    window = webview.create_window(
        "H2H IQX Pipeline",
        url=index_path.as_uri(),
        width=1200,
        height=760,
        min_size=(980, 640),
        js_api=api,
    )
    api.bind_window(window)
    webview.start(debug=False)


if __name__ == "__main__":
    main()
