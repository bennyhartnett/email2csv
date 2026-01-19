import logging
import os
from pathlib import Path
from typing import Any, Mapping


def configure_logging(config: Mapping[str, Any]) -> Path:
    """Configure console + file logging using settings from config."""
    paths = config.get("paths", {}) if isinstance(config, Mapping) else {}
    log_dir = Path(paths.get("log_dir", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    root_logger = logging.getLogger()
    had_handlers = bool(root_logger.handlers)
    if root_logger.level == logging.NOTSET or root_logger.level > logging.INFO:
        root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    if not had_handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    log_file_normalized = os.path.normcase(os.path.abspath(str(log_file)))
    has_file_handler = False
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            base = getattr(handler, "baseFilename", None)
            if base and os.path.normcase(os.path.abspath(str(base))) == log_file_normalized:
                has_file_handler = True
                break

    if not has_file_handler:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Quiet noisy libraries unless debugging
    logging.getLogger("openpyxl").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.getLogger(__name__).info("Logging initialized. Writing to %s", log_file)
    return log_file
