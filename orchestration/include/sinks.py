"""Where finished extract output lands.

A sink stages one file per run and publishes it atomically on commit, together
with a ``<file>.meta.json`` manifest. That two-phase shape is the whole
interface, and it is deliberately the one object storage also gives you: write
to a temporary key, then make it visible under its final key, so a reader never
sees a half-written run.

``LocalSink`` is the only implementation today. A new one is a subclass plus a
``REGISTRY`` entry, selected by ``EXTRACT_SINK`` (config.env) or per source by
the ``sink:`` key in its yml. What an S3/GCS sink additionally needs is written
down in the README's "Swapping infrastructure" section: the load layer's
discovery step still lists a local directory, so a remote sink is two changes,
not one, and pretending otherwise here would be a lie.
"""
import abc
import json
import os
import pathlib

DEFAULT_ROOT = "~/.local/share/vintage-data/extract"


class Sink(abc.ABC):
    """One extract run's output, staged then published.

    Lifecycle, driven by extract_runner: ``writer()`` once, then exactly one of
    ``commit()`` (run succeeded, possibly with zero records) or ``fail()``.
    """

    @abc.abstractmethod
    def writer(self, source: str, dt: str, filename: str):
        """Open the run's staged output as a text file object."""

    @abc.abstractmethod
    def commit(self, meta: dict) -> str:
        """Publish the staged output and write the manifest; return its path."""

    @abc.abstractmethod
    def discard(self) -> None:
        """Drop the staged output after a failed run."""

    @abc.abstractmethod
    def fail(self, meta: dict) -> str:
        """Record a failed run as a marker file; return its path."""


class LocalSink(Sink):
    """NDJSON files under a local root, hive-partitioned (source=<name>/dt=<date>)."""

    type = "local"

    def __init__(self, root: str | None = None):
        root = root or os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_ROOT
        self.root = pathlib.Path(root).expanduser()
        self._staged: pathlib.Path | None = None
        self._final: pathlib.Path | None = None

    def writer(self, source: str, dt: str, filename: str):
        """Open the run's staged output file; finish with commit() or discard()."""
        self._final = self.root / "raw" / f"source={source}" / f"dt={dt}" / filename
        self._staged = self._final.with_name(self._final.name + ".tmp")
        self._staged.parent.mkdir(parents=True, exist_ok=True)
        return open(self._staged, "w", encoding="utf-8")

    def commit(self, meta: dict) -> str:
        """Publish the staged file (atomic rename) and write the manifest.

        A run with zero records is a valid outcome: the empty staged file is
        dropped and only the manifest is written.
        """
        if meta["records"] > 0:
            os.replace(self._staged, self._final)
            meta["path"] = str(self._final)
        else:
            self._staged.unlink(missing_ok=True)
            meta["path"] = None
        manifest = self._final.with_name(self._final.name + ".meta.json")
        manifest.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
        return str(manifest)

    def discard(self):
        """Drop the staged file after a failed run."""
        if self._staged is not None:
            self._staged.unlink(missing_ok=True)

    def fail(self, meta: dict) -> str:
        """Record a failed run as a manifest so monitoring can see failures
        directly instead of inferring them from staleness."""
        self.discard()
        marker = self._final.with_name(self._final.name + ".fail.json")
        marker.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
        return str(marker)


REGISTRY: dict[str, type[Sink]] = {LocalSink.type: LocalSink}


def get_sink(name: str | None = None) -> Sink:
    """Resolve a sink by name; ``None`` means "whatever EXTRACT_SINK says"."""
    name = name or os.environ.get("EXTRACT_SINK") or LocalSink.type
    if name not in REGISTRY:
        raise ValueError(f"unknown sink {name!r} (available: {sorted(REGISTRY)})")
    return REGISTRY[name]()
