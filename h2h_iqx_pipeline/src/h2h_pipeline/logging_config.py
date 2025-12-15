import logging
from pathlib import Path
from typing import Any, Mapping


def configure_logging(config: Mapping[str, Any]) -> Path:
    """Configure console + file logging using settings from config."""
    paths = config.get("paths", {}) if isinstance(config, Mapping) else {}
    log_dir = Path(paths.get("log_dir", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )

    # Quiet noisy libraries unless debugging
    logging.getLogger("openpyxl").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.getLogger(__name__).info("Logging initialized. Writing to %s", log_file)
    return log_file
