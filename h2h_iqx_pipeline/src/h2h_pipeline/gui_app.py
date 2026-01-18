import json
import os
import queue
import re
import sys
import threading
from datetime import date
from pathlib import Path
from typing import Any, Mapping

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

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


class PipelineApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("H2H IQX Pipeline")
        self.minsize(720, 520)

        self._queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._running = False
        self._last_output_root: Path | None = None
        self._last_log_file: Path | None = None

        settings = self._load_settings()
        self.config_path_var = tk.StringVar(value=settings.get("config_path", ""))
        self.input_root_var = tk.StringVar(value=settings.get("input_root", ""))
        self.output_root_var = tk.StringVar(value=settings.get("output_root", ""))
        self.month_var = tk.StringVar(value=settings.get("month", date.today().strftime("%Y-%m")))
        self.status_var = tk.StringVar(value="Idle")

        self._build_ui()
        self._prefill_from_config()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(100, self._drain_queue)

    def _build_ui(self) -> None:
        main = ttk.Frame(self, padding=12)
        main.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        main.columnconfigure(1, weight=1)

        row = 0
        self._add_browse_row(
            main,
            row,
            "Config file",
            self.config_path_var,
            self._browse_config,
            "Browse",
        )
        row += 1
        self._add_browse_row(
            main,
            row,
            "Input root folder",
            self.input_root_var,
            self._browse_input_root,
            "Browse",
        )
        row += 1
        self._add_browse_row(
            main,
            row,
            "Output folder",
            self.output_root_var,
            self._browse_output_root,
            "Browse",
        )
        row += 1

        ttk.Label(main, text="Month (YYYY-MM)").grid(row=row, column=0, sticky="w", pady=(6, 6))
        ttk.Entry(main, textvariable=self.month_var).grid(row=row, column=1, sticky="ew", pady=(6, 6))
        row += 1

        buttons = ttk.Frame(main)
        buttons.grid(row=row, column=0, columnspan=3, sticky="w", pady=(6, 12))

        self.run_button = ttk.Button(buttons, text="Run pipeline", command=self._start_run)
        self.run_button.grid(row=0, column=0, padx=(0, 8))

        self.open_output_button = ttk.Button(
            buttons, text="Open output folder", command=self._open_output, state="disabled"
        )
        self.open_output_button.grid(row=0, column=1, padx=(0, 8))

        self.open_log_button = ttk.Button(
            buttons, text="Open log file", command=self._open_log, state="disabled"
        )
        self.open_log_button.grid(row=0, column=2, padx=(0, 8))

        row += 1
        ttk.Label(main, text="Status").grid(row=row, column=0, sticky="w")
        ttk.Label(main, textvariable=self.status_var).grid(row=row, column=1, sticky="w")
        row += 1

        ttk.Label(main, text="Run log (last 200 lines)").grid(row=row, column=0, sticky="w", pady=(8, 2))
        row += 1
        self.log_box = ScrolledText(main, height=14, wrap="word", state="disabled")
        self.log_box.grid(row=row, column=0, columnspan=3, sticky="nsew")
        main.rowconfigure(row, weight=1)

    def _add_browse_row(
        self,
        parent: ttk.Frame,
        row: int,
        label: str,
        variable: tk.StringVar,
        command: callable,
        button_text: str,
    ) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=(6, 6))
        ttk.Entry(parent, textvariable=variable).grid(row=row, column=1, sticky="ew", pady=(6, 6))
        ttk.Button(parent, text=button_text, command=command).grid(
            row=row, column=2, sticky="ew", padx=(8, 0), pady=(6, 6)
        )

    def _browse_config(self) -> None:
        initial = self.config_path_var.get() or str(_resource_path("config/example_config.yml"))
        path = filedialog.askopenfilename(
            title="Select config file",
            initialdir=str(Path(initial).parent) if initial else None,
            filetypes=[("YAML files", "*.yml;*.yaml"), ("All files", "*.*")],
        )
        if path:
            self.config_path_var.set(path)
            self._prefill_from_config(force=True)

    def _browse_input_root(self) -> None:
        path = filedialog.askdirectory(title="Select input root folder")
        if path:
            self.input_root_var.set(path)

    def _browse_output_root(self) -> None:
        path = filedialog.askdirectory(title="Select output folder")
        if path:
            self.output_root_var.set(path)

    def _prefill_from_config(self, force: bool = False) -> None:
        config_path = self.config_path_var.get().strip()
        if not config_path:
            return
        cfg_path = Path(config_path)
        if not cfg_path.exists():
            return
        try:
            cfg = _load_yaml(cfg_path)
            if not isinstance(cfg, Mapping):
                return
            normalize_config_paths(cfg, cfg_path)
        except Exception:
            return

        paths = cfg.get("paths", {})
        run_cfg = cfg.get("run", {})
        if force or not self.input_root_var.get():
            if paths.get("input_root"):
                self.input_root_var.set(paths["input_root"])
        if force or not self.output_root_var.get():
            if paths.get("output_root"):
                self.output_root_var.set(paths["output_root"])
        if force or not self.month_var.get():
            if run_cfg.get("current_month"):
                self.month_var.set(run_cfg["current_month"])

    def _start_run(self) -> None:
        if self._running:
            return

        config_path = self.config_path_var.get().strip()
        if not config_path:
            messagebox.showerror("Missing config", "Please select a config file.")
            return
        if not Path(config_path).exists():
            messagebox.showerror("Missing config", f"Config file not found: {config_path}")
            return

        month = self.month_var.get().strip()
        if month and not MONTH_PATTERN.match(month):
            messagebox.showerror("Invalid month", "Month must be in YYYY-MM format.")
            return

        self._set_running(True)
        self._clear_log()
        self._append_log("Starting pipeline run...")

        thread = threading.Thread(
            target=self._run_pipeline_worker,
            args=(config_path, self.input_root_var.get().strip(), self.output_root_var.get().strip(), month),
            daemon=True,
        )
        thread.start()

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

            cfg = load_config(Path(config_path), overrides=overrides if overrides["paths"] else None)

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

            self._queue.put(("status", "Running pipeline..."))
            self._queue.put(("log", f"Config: {config_path}"))
            self._queue.put(("log", f"Input root: {input_root}"))
            self._queue.put(("log", f"Output root: {output_root}"))
            self._queue.put(("log", f"Month: {month}"))

            run_pipeline(month=month, input_root=input_root, config=cfg)

            log_file = Path(paths_cfg["log_dir"]) / "pipeline.log"
            log_lines = _tail_lines(log_file)
            self._queue.put(("log", "Pipeline run completed."))
            if log_lines:
                self._queue.put(("log", "---- Pipeline log (tail) ----"))
                for line in log_lines:
                    self._queue.put(("log", line))

            self._queue.put(
                ("done", {"output_root": output_root, "log_file": log_file, "message": "Run complete"})
            )
        except Exception as exc:
            self._queue.put(("error", str(exc)))

    def _drain_queue(self) -> None:
        try:
            while True:
                kind, payload = self._queue.get_nowait()
                if kind == "log":
                    self._append_log(payload)
                elif kind == "status":
                    self.status_var.set(payload)
                elif kind == "done":
                    self._handle_done(payload)
                elif kind == "error":
                    self._handle_error(payload)
        except queue.Empty:
            pass
        self.after(100, self._drain_queue)

    def _handle_done(self, payload: Mapping[str, Any]) -> None:
        self.status_var.set(payload.get("message", "Done"))
        self._last_output_root = payload.get("output_root")
        self._last_log_file = payload.get("log_file")
        if self._last_output_root:
            self.open_output_button.configure(state="normal")
        if self._last_log_file:
            self.open_log_button.configure(state="normal")
        self._set_running(False)
        self._save_settings()

    def _handle_error(self, message: str) -> None:
        self.status_var.set("Error")
        self._append_log(f"ERROR: {message}")
        self._set_running(False)
        messagebox.showerror("Pipeline error", message)

    def _set_running(self, running: bool) -> None:
        self._running = running
        state = "disabled" if running else "normal"
        for widget in (self.run_button,):
            widget.configure(state=state)

    def _append_log(self, message: str) -> None:
        self.log_box.configure(state="normal")
        self.log_box.insert("end", f"{message}\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _clear_log(self) -> None:
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")

    def _open_output(self) -> None:
        if self._last_output_root and Path(self._last_output_root).exists():
            if hasattr(os, "startfile"):
                os.startfile(self._last_output_root)  # type: ignore[attr-defined]

    def _open_log(self) -> None:
        if self._last_log_file and Path(self._last_log_file).exists():
            if hasattr(os, "startfile"):
                os.startfile(self._last_log_file)  # type: ignore[attr-defined]

    def _load_settings(self) -> dict[str, str]:
        if SETTINGS_PATH.exists():
            try:
                return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_settings(self) -> None:
        settings = {
            "config_path": self.config_path_var.get().strip(),
            "input_root": self.input_root_var.get().strip(),
            "output_root": self.output_root_var.get().strip(),
            "month": self.month_var.get().strip(),
        }
        try:
            SETTINGS_PATH.write_text(json.dumps(settings, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _on_close(self) -> None:
        if not self._running:
            self._save_settings()
        self.destroy()


def main() -> None:
    app = PipelineApp()
    app.mainloop()


if __name__ == "__main__":
    main()
