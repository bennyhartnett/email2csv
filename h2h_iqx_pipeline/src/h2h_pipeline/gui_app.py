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
import tkinter.font as tkfont
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
        self.geometry("1100x700")
        self.minsize(1000, 640)

        self._queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._running = False
        self._last_output_root: Path | None = None
        self._last_log_file: Path | None = None

        self._colors: dict[str, str] = {}
        self._apply_theme()

        settings = self._load_settings()
        self.config_path_var = tk.StringVar(value=settings.get("config_path", ""))
        self.input_root_var = tk.StringVar(value=settings.get("input_root", ""))
        self.output_root_var = tk.StringVar(value=settings.get("output_root", ""))
        self.month_var = tk.StringVar(value=settings.get("month", date.today().strftime("%Y-%m")))
        self.status_var = tk.StringVar(value="Idle")
        self.status_label: ttk.Label | None = None
        self.progress: ttk.Progressbar | None = None

        self._build_ui()
        self._prefill_from_config()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(100, self._drain_queue)

    def _pick_font(self, candidates: list[str]) -> str:
        available = set(tkfont.families(self))
        for name in candidates:
            if name in available:
                return name
        return candidates[-1]

    def _apply_theme(self) -> None:
        colors = {
            "bg": "#0b0f14",
            "header": "#0f172a",
            "card": "#111827",
            "text": "#e5e7eb",
            "muted": "#9ca3af",
            "accent": "#22d3ee",
            "accent_dark": "#0891b2",
            "accent_light": "#67e8f9",
            "input": "#0b1220",
            "border": "#1f2937",
            "log_bg": "#070a0f",
            "log_fg": "#cbd5e1",
            "pill_idle": "#1f2937",
            "pill_run": "#0b3a4d",
            "pill_done": "#0b3b2b",
            "pill_error": "#4c1d1d",
        }
        self._colors = colors

        self.configure(bg=colors["bg"])
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        base_font = self._pick_font(["Segoe UI Variable Text", "Segoe UI", "Helvetica"])
        title_font = self._pick_font(["Segoe UI Variable Display", "Segoe UI", "Helvetica"])
        mono_font = self._pick_font(["Cascadia Mono", "Consolas", "Courier New"])

        self.body_font = tkfont.Font(family=base_font, size=10)
        self.title_font = tkfont.Font(family=title_font, size=20, weight="bold")
        self.section_font = tkfont.Font(family=base_font, size=12, weight="bold")
        self.button_font = tkfont.Font(family=base_font, size=10, weight="bold")
        self.pill_font = tkfont.Font(family=base_font, size=9, weight="bold")
        self.mono_font = tkfont.Font(family=mono_font, size=9)

        style.configure("TLabel", background=colors["bg"], foreground=colors["text"], font=self.body_font)
        style.configure("App.TFrame", background=colors["bg"])
        style.configure("Header.TFrame", background=colors["header"])
        style.configure("Card.TFrame", background=colors["card"])
        style.configure("Card.TLabel", background=colors["card"], foreground=colors["text"], font=self.body_font)
        style.configure("Card.Title.TLabel", background=colors["card"], foreground=colors["text"], font=self.section_font)
        style.configure("Card.Subtitle.TLabel", background=colors["card"], foreground=colors["muted"], font=self.body_font)
        style.configure("Header.Title.TLabel", background=colors["header"], foreground=colors["text"], font=self.title_font)
        style.configure(
            "Header.Subtitle.TLabel", background=colors["header"], foreground=colors["muted"], font=self.body_font
        )

        style.configure("Modern.TEntry", fieldbackground=colors["input"], foreground=colors["text"])

        style.configure(
            "Accent.TButton",
            background=colors["accent"],
            foreground="#0b0f14",
            borderwidth=0,
            padding=(14, 6),
            font=self.button_font,
        )
        style.map(
            "Accent.TButton",
            background=[("pressed", colors["accent_dark"]), ("active", colors["accent_light"])],
            foreground=[("disabled", colors["muted"])],
        )

        style.configure(
            "Secondary.TButton",
            background=colors["card"],
            foreground=colors["text"],
            borderwidth=1,
            padding=(12, 6),
            font=self.button_font,
        )
        style.map(
            "Secondary.TButton",
            background=[("active", colors["input"])],
            foreground=[("disabled", colors["muted"])],
        )

        style.configure(
            "Pill.Idle.TLabel",
            background=colors["pill_idle"],
            foreground=colors["text"],
            padding=(10, 4),
            font=self.pill_font,
        )
        style.configure(
            "Pill.Running.TLabel",
            background=colors["pill_run"],
            foreground=colors["text"],
            padding=(10, 4),
            font=self.pill_font,
        )
        style.configure(
            "Pill.Done.TLabel",
            background=colors["pill_done"],
            foreground=colors["text"],
            padding=(10, 4),
            font=self.pill_font,
        )
        style.configure(
            "Pill.Error.TLabel",
            background=colors["pill_error"],
            foreground=colors["text"],
            padding=(10, 4),
            font=self.pill_font,
        )

        style.configure("TSeparator", background=colors["border"])

        style.configure(
            "Accent.Horizontal.TProgressbar",
            troughcolor=colors["input"],
            background=colors["accent"],
            bordercolor=colors["border"],
            lightcolor=colors["accent"],
            darkcolor=colors["accent_dark"],
        )

    def _build_ui(self) -> None:
        main = ttk.Frame(self, padding=20, style="App.TFrame")
        main.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        main.columnconfigure(0, weight=6)
        main.columnconfigure(1, weight=5)
        main.rowconfigure(1, weight=1)

        header = ttk.Frame(main, padding=(18, 16), style="Header.TFrame")
        header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 16))
        header.columnconfigure(0, weight=1)

        ttk.Label(header, text="H2H IQX Pipeline", style="Header.Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            text="Build, clean, and export IQX-ready CSVs in one run.",
            style="Header.Subtitle.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

        left = ttk.Frame(main, padding=16, style="Card.TFrame")
        left.grid(row=1, column=0, sticky="nsew", padx=(0, 12))
        left.columnconfigure(0, minsize=140)
        left.columnconfigure(1, weight=1, minsize=320)
        left.columnconfigure(2, minsize=110)

        ttk.Label(left, text="Run configuration", style="Card.Title.TLabel").grid(
            row=0, column=0, columnspan=3, sticky="w"
        )
        ttk.Label(
            left, text="Select your config and data folders, then pick a month.", style="Card.Subtitle.TLabel"
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(2, 10))

        row = 2
        self._add_browse_row(
            left,
            row,
            "Config file",
            self.config_path_var,
            self._browse_config,
            "Browse",
        )
        row += 1
        self._add_browse_row(
            left,
            row,
            "Input root folder",
            self.input_root_var,
            self._browse_input_root,
            "Browse",
        )
        row += 1
        self._add_browse_row(
            left,
            row,
            "Output folder",
            self.output_root_var,
            self._browse_output_root,
            "Browse",
        )
        row += 1

        ttk.Label(left, text="Month (YYYY-MM)", style="Card.TLabel").grid(row=row, column=0, sticky="w", pady=(8, 6))
        ttk.Entry(left, textvariable=self.month_var, style="Modern.TEntry").grid(
            row=row, column=1, columnspan=2, sticky="ew", pady=(8, 6)
        )
        row += 1

        ttk.Separator(left).grid(row=row, column=0, columnspan=3, sticky="ew", pady=(8, 12))
        row += 1

        buttons = ttk.Frame(left, style="Card.TFrame")
        buttons.grid(row=row, column=0, columnspan=3, sticky="ew")
        buttons.columnconfigure(0, weight=1, uniform="actions")
        buttons.columnconfigure(1, weight=1, uniform="actions")
        buttons.columnconfigure(2, weight=1, uniform="actions")

        self.run_button = ttk.Button(buttons, text="Run pipeline", command=self._start_run, style="Accent.TButton")
        self.run_button.grid(row=0, column=0, padx=(0, 8), sticky="ew")

        self.open_output_button = ttk.Button(
            buttons, text="Open output folder", command=self._open_output, state="disabled", style="Secondary.TButton"
        )
        self.open_output_button.grid(row=0, column=1, padx=(0, 8), sticky="ew")

        self.open_log_button = ttk.Button(
            buttons, text="Open log file", command=self._open_log, state="disabled", style="Secondary.TButton"
        )
        self.open_log_button.grid(row=0, column=2, sticky="ew")

        row += 1
        self.progress = ttk.Progressbar(left, mode="indeterminate", style="Accent.Horizontal.TProgressbar")
        self.progress.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(10, 6))

        row += 1
        status_row = ttk.Frame(left, style="Card.TFrame")
        status_row.grid(row=row, column=0, columnspan=3, sticky="w", pady=(4, 0))
        ttk.Label(status_row, text="Status", style="Card.TLabel").grid(row=0, column=0, sticky="w")
        self.status_label = ttk.Label(status_row, textvariable=self.status_var, style="Pill.Idle.TLabel")
        self.status_label.grid(row=0, column=1, sticky="w", padx=(8, 0))

        right = ttk.Frame(main, padding=16, style="Card.TFrame")
        right.grid(row=1, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(2, weight=1)

        ttk.Label(right, text="Activity", style="Card.Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(right, text="Run log (last 200 lines)", style="Card.Subtitle.TLabel").grid(
            row=1, column=0, sticky="w", pady=(2, 8)
        )

        self.log_box = ScrolledText(
            right,
            height=18,
            wrap="word",
            state="disabled",
            font=self.mono_font,
            background=self._colors["log_bg"],
            foreground=self._colors["log_fg"],
            relief="flat",
            borderwidth=0,
            highlightthickness=1,
            highlightbackground=self._colors["border"],
            highlightcolor=self._colors["border"],
            insertbackground=self._colors["log_fg"],
        )
        self.log_box.grid(row=2, column=0, sticky="nsew")

    def _add_browse_row(
        self,
        parent: ttk.Frame,
        row: int,
        label: str,
        variable: tk.StringVar,
        command: callable,
        button_text: str,
    ) -> None:
        ttk.Label(parent, text=label, style="Card.TLabel").grid(row=row, column=0, sticky="w", pady=(6, 6))
        ttk.Entry(parent, textvariable=variable, style="Modern.TEntry").grid(
            row=row, column=1, sticky="ew", pady=(6, 6)
        )
        ttk.Button(parent, text=button_text, command=command, style="Secondary.TButton").grid(
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
                    self._set_status(payload)
                elif kind == "done":
                    self._handle_done(payload)
                elif kind == "error":
                    self._handle_error(payload)
        except queue.Empty:
            pass
        self.after(100, self._drain_queue)

    def _handle_done(self, payload: Mapping[str, Any]) -> None:
        self._set_status(payload.get("message", "Done"))
        self._last_output_root = payload.get("output_root")
        self._last_log_file = payload.get("log_file")
        if self._last_output_root:
            self.open_output_button.configure(state="normal")
        if self._last_log_file:
            self.open_log_button.configure(state="normal")
        self._set_running(False)
        self._save_settings()

    def _handle_error(self, message: str) -> None:
        self._set_status("Error")
        self._append_log(f"ERROR: {message}")
        self._set_running(False)
        messagebox.showerror("Pipeline error", message)

    def _set_running(self, running: bool) -> None:
        self._running = running
        state = "disabled" if running else "normal"
        for widget in (self.run_button,):
            widget.configure(state=state)
        if self.progress:
            if running:
                self.progress.start(12)
                self._set_status("Running...")
            else:
                self.progress.stop()

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

    def _status_style_for_text(self, text: str) -> str:
        lowered = text.lower()
        if "error" in lowered:
            return "Pill.Error.TLabel"
        if "running" in lowered:
            return "Pill.Running.TLabel"
        if "complete" in lowered or "done" in lowered:
            return "Pill.Done.TLabel"
        return "Pill.Idle.TLabel"

    def _set_status(self, text: str) -> None:
        self.status_var.set(text)
        if self.status_label is not None:
            self.status_label.configure(style=self._status_style_for_text(text))

    def _on_close(self) -> None:
        if not self._running:
            self._save_settings()
        self.destroy()


def main() -> None:
    app = PipelineApp()
    app.mainloop()


if __name__ == "__main__":
    main()
