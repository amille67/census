"""Pipeline registry for managing available ingest pipelines."""

from pathlib import Path

import yaml

from backend.utils.logging import get_logger

logger = get_logger("orchestration.registry")


def load_source_registry(registry_dir: Path = None) -> dict:
    """Load all source registry YAML files."""
    if registry_dir is None:
        registry_dir = Path("configs/source_registry")

    sources = {}
    for yaml_file in registry_dir.glob("*.yaml"):
        if yaml_file.name == "template_source.yaml":
            continue
        with open(yaml_file) as f:
            config = yaml.safe_load(f)
        slug = config.get("source", {}).get("slug", yaml_file.stem)
        sources[slug] = config
        logger.info("Registered source: %s", slug)

    return sources


def get_source_config(slug: str, registry_dir: Path = None) -> dict:
    """Get configuration for a specific source."""
    sources = load_source_registry(registry_dir)
    if slug not in sources:
        raise KeyError(f"Source '{slug}' not found in registry. Available: {list(sources.keys())}")
    return sources[slug]
