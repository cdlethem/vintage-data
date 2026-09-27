"""Source artifacts stay bounded in transit without relaxing extraction limits."""
import gzip
import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from airflow.providers.vintage.bot_dashboard import repository_ops
from bots.admitted_runner import _extract_source


class ExtractionError(RuntimeError):
    def __init__(self, code, retry_class):
        super().__init__(code)
        self.code = code


class SourceArtifactTests(unittest.TestCase):
    def archive(self, content=b'hello', *, kind=None, size=None):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode='w') as archive:
            member = tarfile.TarInfo('source.txt')
            if kind:
                member.type = kind
                member.linkname = '/etc/passwd'
            member.size = len(content) if size is None else size
            archive.addfile(member, io.BytesIO(content) if kind is None else None)
        return buffer.getvalue()

    def test_large_source_compressed_deterministically_and_roundtrips(self):
        content = b'engineering source material\n' * 400_000
        raw = self.archive(content)
        self.assertGreater(len(raw), 8 * 1024 * 1024)
        stored = []
        config = SimpleNamespace(project='org/repo', provider='github', base_branch='main')
        def put(session, *, content, **kwargs):
            stored.append(content)
            return {'sha256': hashlib.sha256(content).hexdigest(), 'byte_count': len(content)}
        with patch.object(repository_ops, 'load_repository_config', return_value=config), \
             patch.object(repository_ops, '_materialize', return_value=(Path('/tmp'), 'a'*40)), \
             patch.object(repository_ops.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout=raw)), \
             patch.object(repository_ops, 'put_artifact', side_effect=put):
            for _ in range(2):
                execution = SimpleNamespace(source_artifact_sha256=None, execution_id='e'*64)
                repository_ops.create_source_artifact(Mock(), execution)
        self.assertEqual(stored[0], stored[1])
        self.assertLess(len(stored[0]), 8 * 1024 * 1024)
        self.assertEqual(gzip.decompress(stored[0]), raw)
        self.assertEqual(stored[0][4:8], b'\x00'*4)
        with tempfile.TemporaryDirectory() as temporary:
            work = _extract_source(stored[0], Path(temporary), ExtractionError)
            self.assertEqual((work / 'source.txt').read_bytes(), content)

    def test_legacy_uncompressed_archive_still_supported(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = _extract_source(self.archive(), Path(temporary), ExtractionError)
            self.assertEqual((work / 'source.txt').read_bytes(), b'hello')

    def test_compressed_symlinks_and_unpacked_oversize_still_rejected(self):
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                with self.assertRaisesRegex(ExtractionError, 'source_archive_unsafe'):
                    _extract_source(gzip.compress(self.archive(b'', kind=kind), mtime=0), Path(temporary), ExtractionError)
        # Only a header is needed: reject the declared size before decompressing its body.
        member = tarfile.TarInfo('large.txt')
        member.size = 512 * 1024 * 1024 + 1
        oversized = gzip.compress(member.tobuf() + b'\x00'*1024, mtime=0)
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ExtractionError, 'source_archive_outside_bounds'):
                _extract_source(oversized, Path(temporary), ExtractionError)


if __name__ == '__main__':
    unittest.main()
