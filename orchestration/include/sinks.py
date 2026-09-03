"""Where finished extract output lands.

A sink stages one file per run and publishes it atomically on commit, together
with a ``<file>.meta.json`` manifest. LocalSink is the only implementation
today; an S3/GCS sink later is a new class with the same two methods, selected
by the ``sink:`` key in a source's yml.
"""
import json
import os
import pathlib

DEFAULT_ROOT = "~/dev/data/extract"


class LocalSink:
    """NDJSON files under a local root, hive-partitioned (source=<name>/dt=<date>)."""

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


def get_sink(name: str = "local"):
    if name == "local":
        return LocalSink()
    raise ValueError(f"unknown sink {name!r} (supported: local)")
