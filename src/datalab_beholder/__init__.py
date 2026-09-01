"""datalab-beholder: Filesystem watcher daemon for datalab instances."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("datalab-beholder")
except PackageNotFoundError:  # pragma: no cover - running from a source tree
    # Matches the setuptools_scm fallback_version in pyproject.toml.
    __version__ = "0.1.0"
