"""Environment variable and configuration loading."""

import os
from pathlib import Path
from typing import Optional

import yaml


def load_dotenv():
    """Load .env file if it exists."""
    try:
        from dotenv import load_dotenv as _load
        _load()
    except ImportError:
        pass


def get_data_root() -> Path:
    """Get the data root directory from environment or default."""
    return Path(os.environ.get("DATA_ROOT", "./data"))


def get_census_api_key() -> Optional[str]:
    """Get Census API key from environment."""
    return os.environ.get("CENSUS_API_KEY")


def load_yaml_config(config_name: str) -> dict:
    """Load a YAML config file from the configs directory."""
    path = Path("configs") / config_name
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def get_paths_config() -> dict:
    """Load the paths configuration."""
    return load_yaml_config("paths.yaml")


def resolve_data_path(relative_path: str) -> Path:
    """Resolve a relative data path to an absolute path."""
    return get_data_root() / relative_path
