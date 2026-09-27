"""Exercise promotion and bounded recovery against local Git repositories only."""

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from sync import DeploymentError, Sync


def git(where: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=where, text=True, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.strip()


class DeploymentTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        self.remote = base / "remote.git"
        subprocess.run(["git", "init", "--bare", "--initial-branch=main", str(self.remote)],
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.root = base / "live"
        subprocess.run(["git", "clone", str(self.remote), str(self.root)],
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        git(self.root, "config", "user.name", "Local release")
        git(self.root, "config", "user.email", "release@example.test")
        (self.root / ".gitignore").write_text(
            "orchestration/config.env\norchestration/airflow.env\n"
            "orchestration/airflow.secrets.env\norchestration/airflow_home/\n"
            "orchestration/.venv/\norchestration/generated/\nbots/models.yml\n"
        )
        (self.root / "revision.txt").write_text("old\n")
        git(self.root, "add", ".gitignore", "revision.txt")
        git(self.root, "commit", "-m", "initial")
        git(self.root, "push", "origin", "main")
        self.old = git(self.root, "rev-parse", "HEAD")
        self.sync = Sync(self.root)
        # No build/network in the test; wheels are per-revision release artifacts.
        self.installations = []
        self.sync.stage = self.fake_stage
        self.sync.install = self.installations.append
        self.sync.prepare_runtime = lambda: None
        self.private = self.root / "orchestration/airflow.env"
        self.private.parent.mkdir(parents=True)
        self.private.write_text("PRIVATE=local-only\n")
        self.advance("new")

    def advance(self, text: str) -> None:
        clone = Path(self.temp.name) / "publisher"
        if not clone.exists():
            subprocess.run(["git", "clone", str(self.remote), str(clone)],
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            git(clone, "config", "user.name", "Publisher")
            git(clone, "config", "user.email", "publisher@example.test")
        (clone / "revision.txt").write_text(text + "\n")
        git(clone, "add", "revision.txt")
        git(clone, "commit", "-m", text)
        git(clone, "push", "origin", "main")
        self.new = git(clone, "rev-parse", "HEAD")

    def fake_stage(self, sha: str, *, validate: bool) -> Path:
        wheel = self.sync.state_dir / sha / "provider.whl"
        wheel.parent.mkdir(parents=True, exist_ok=True)
        wheel.write_text(sha)
        return wheel

    def test_deploy_and_bounded_manual_rollback_preserve_ignored_runtime_config(self):
        with redirect_stdout(StringIO()) as output:
            self.assertEqual(self.sync.deploy(), 10)
        self.assertIn("DEPLOYED_TO=" + self.new, output.getvalue())
        self.assertEqual(git(self.root, "rev-parse", "HEAD"), self.new)
        self.assertEqual(self.private.read_text(), "PRIVATE=local-only\n")
        with self.assertRaisesRegex(DeploymentError, "recorded previous"):
            self.sync.rollback(self.new)
        self.assertEqual(self.sync.rollback(self.old), 10)
        self.assertEqual(git(self.root, "rev-parse", "HEAD"), self.old)
        self.assertEqual((self.root / "revision.txt").read_text(), "old\n")
        self.assertEqual(self.private.read_text(), "PRIVATE=local-only\n")
        self.assertEqual(self.installations[-1].read_text(), self.old)
        self.assertEqual(self.sync.record()["status"], "rejected")
        with self.assertRaisesRegex(DeploymentError, "rejected release"):
            self.sync.check()

    def test_failed_install_restores_old_checkout_and_old_wheel(self):
        attempts = []
        def fail_once(wheel: Path):
            attempts.append(wheel.read_text())
            if len(attempts) == 1:
                raise DeploymentError("wheel installation failed")
        self.sync.install = fail_once
        with self.assertRaisesRegex(DeploymentError, "wheel installation failed"):
            self.sync.deploy()
        self.assertEqual(attempts, [self.new, self.old])
        self.assertEqual(git(self.root, "rev-parse", "HEAD"), self.old)
        self.assertEqual((self.root / "revision.txt").read_text(), "old\n")
        self.assertEqual(self.private.read_text(), "PRIVATE=local-only\n")
        self.assertEqual(self.sync.record()["status"], "rejected")
        with self.assertRaisesRegex(DeploymentError, "rejected release"):
            self.sync.deploy()
        self.advance("fixed")
        with redirect_stdout(StringIO()):
            self.assertEqual(self.sync.deploy(), 10)
        self.assertEqual(git(self.root, "rev-parse", "HEAD"), self.new)

    def test_refuses_dirty_or_diverged_main(self):
        (self.root / "untracked.txt").write_text("do not delete")
        with self.assertRaisesRegex(DeploymentError, "untracked"):
            self.sync.check()
        (self.root / "untracked.txt").unlink()
        (self.root / "revision.txt").write_text("local revision\n")
        git(self.root, "add", "revision.txt")
        git(self.root, "commit", "-m", "local divergence")
        with self.assertRaisesRegex(DeploymentError, "diverges"):
            self.sync.check()

    def test_remote_history_rewrite_refused(self):
        self.assertEqual(self.sync.check(), (self.old, self.new))
        # Simulate a rewritten bare remote without force-pushing anything.
        git(self.remote, "update-ref", "refs/heads/main", self.old, self.new)
        with self.assertRaises(DeploymentError):
            self.sync.check()
        self.assertEqual(git(self.root, "rev-parse", "HEAD"), self.old)
        self.assertIsNone(self.sync.record())

    def test_private_file_newly_tracked_in_main_is_rejected(self):
        clone = Path(self.temp.name) / "publisher"
        private = clone / "orchestration/airflow.env"
        private.parent.mkdir(parents=True)
        private.write_text("SHOULD_NOT_OVERWRITE=1\n")
        git(clone, "add", "-f", "orchestration/airflow.env")
        git(clone, "commit", "-m", "accidentally track private config")
        git(clone, "push", "origin", "main")
        with self.assertRaisesRegex(DeploymentError, "tracks private"):
            self.sync.deploy()
        self.assertEqual(self.private.read_text(), "PRIVATE=local-only\n")
        self.assertEqual(git(self.root, "rev-parse", "HEAD"), self.old)

    def test_noop_and_no_arg_rollback_require_recorded_revision(self):
        with self.assertRaisesRegex(DeploymentError, "recorded previous"):
            self.sync.rollback(None)
        with redirect_stdout(StringIO()):
            self.assertEqual(self.sync.deploy(), 10)
        with redirect_stdout(StringIO()) as output:
            self.assertEqual(self.sync.deploy(), 0)
        self.assertIn("STATUS=no-op", output.getvalue())
        self.assertEqual(self.sync.rollback(None), 10)

    def test_stage_uses_git_worktree_without_ignored_config_or_inherited_secrets(self):
        self.sync.check()  # Discover the published revision before staging it.
        self.sync.stage = Sync.stage.__get__(self.sync)
        self.sync.validate = lambda *_: None
        original = self.sync.run
        observed = []
        def fake_build(args, *, cwd=None, env=None):
            if args[0] in ("corepack", "uv"):
                observed.append((Path(cwd), env))
                if args[:2] == ["uv", "build"]:
                    wheel_dir = Path(args[args.index("--out-dir") + 1])
                    wheel_dir.mkdir()
                    (wheel_dir / "provider.whl").write_text("staged")
                return ""
            return original(args, cwd=cwd, env=env)
        self.sync.run = fake_build
        with mock.patch.dict("os.environ", {"MY_SECRET_TOKEN": "not-in-stage"}):
            wheel = self.sync.stage(self.new, validate=True)
        self.assertEqual(wheel.read_text(), "staged")
        self.assertTrue(observed)
        for cwd, env in observed:
            self.assertNotEqual(cwd, self.root)
            self.assertFalse((cwd / "orchestration/airflow.env").exists())
            self.assertNotIn("MY_SECRET_TOKEN", env)
        self.assertEqual(self.private.read_text(), "PRIVATE=local-only\n")


if __name__ == "__main__":
    unittest.main()
