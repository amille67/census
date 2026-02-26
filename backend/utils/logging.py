"""Logging configuration for the ETL pipeline."""

import logging
import logging.config
from pathlib import Path

import yaml


def setup_logging(config_path: str = "configs/logging.yaml", level: str = "INFO"):
    """Configure logging from YAML config file."""
    config_file = Path(config_path)
    if config_file.exists():
        with open(config_file) as f:
            config = yaml.safe_load(f)
        log_dir = Path(config.get("handlers", {}).get("file", {}).get("filename", "")).parent
        if log_dir and not log_dir.exists():
            log_dir.mkdir(parents=True, exist_ok=True)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(
            level=getattr(logging, level.upper(), logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name under the backend namespace."""
    return logging.getLogger(f"backend.{name}")
