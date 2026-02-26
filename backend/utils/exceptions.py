"""Custom exceptions for the ETL pipeline."""


class PipelineError(Exception):
    """Base exception for all pipeline errors."""


class GeoidValidationError(PipelineError):
    """Raised when GEOID validation fails."""


class VintageError(PipelineError):
    """Raised when data vintage does not match expected 2020 Census geography."""


class JoinCardinalityError(PipelineError):
    """Raised when a join produces unexpected row multiplication."""


class SchemaValidationError(PipelineError):
    """Raised when a DataFrame does not match its expected schema."""


class SourceConfigError(PipelineError):
    """Raised when a source registry YAML is invalid."""


class DownloadError(PipelineError):
    """Raised when a download fails after retries."""


class QACheckError(PipelineError):
    """Raised when a QA check fails."""
