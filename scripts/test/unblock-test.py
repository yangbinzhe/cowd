#!/usr/bin/env python3
"""Exercise recoverable preflight cases without touching Docker or evidence."""
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import socket
import stat
import subprocess
import tempfile
import unittest
from unittest import mock

SCRIPTS = Path(__file__).resolve().parent

class UnblockTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cowd-unblock-test-', dir='/dev/shm')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = dict(os.environ)
        for name in ('CARGO_HOME', 'CARGO_TARGET_DIR', 'SCCACHE_DIR', 'COWD_GATE_LOG_DIR'):
            self.env.pop(name, None)
        self.env['COWD_BUILD_ROOT'] = str(self.root / 'build')

    def run_gate(self, *args):
        return subprocess.run(['bash', str(SCRIPTS / 'unblock.sh'), *map(str, args)],
                              env=self.env, text=True, capture_output=True)

    def test_private_root_repairs_mode_and_is_idempotent(self):
        root = self.root / 'private'
        root.mkdir(mode=0o755)
        root.chmod(0o755)
        for _ in range(2):
            self.assertEqual(self.run_gate('private-root', root).returncode, 0)
            self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)

    def test_private_root_rejects_symlink(self):
        target = self.root / 'target'
        target.mkdir()
        link = self.root / 'link'
        link.symlink_to(target)
        self.assertNotEqual(self.run_gate('private-root', link).returncode, 0)

    def test_launcher_private_root_warns_when_chmod_is_unsupported(self):
        launcher = SCRIPTS.parent / 'scenarios' / 'launcher-034.py'
        spec = importlib.util.spec_from_file_location('launcher_034', launcher)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        root = self.root / 'launcher-private'
        root.mkdir(mode=0o755)
        root.chmod(0o755)
        warning = io.StringIO()
        with mock.patch.object(Path, 'chmod', side_effect=PermissionError('unsupported')):
            with contextlib.redirect_stderr(warning):
                module.private_dir(root)
        self.assertIn('warning:', warning.getvalue())
        link = self.root / 'launcher-link'
        link.symlink_to(root)
        with self.assertRaises(ValueError):
            module.private_dir(link)

    def test_port_returns_a_local_bindable_port(self):
        result = self.run_gate('port')
        self.assertEqual(result.returncode, 0, result.stderr)
        with socket.socket() as s:
            s.bind(('127.0.0.1', int(result.stdout)))

    def test_unknown_gate_fails_closed(self):
        self.assertNotEqual(self.run_gate('missing-gate').returncode, 0)

    def test_build_env_repairs_root_disk_overrides_but_keeps_owned_target(self):
        self.env.update(CARGO_HOME='/tmp/cowd-wrong-home',
                        CARGO_TARGET_DIR=str(self.root / 'build' / 'leg-target'),
                        SCCACHE_DIR='/tmp/cowd-wrong-cache',
                        COWD_GATE_LOG_DIR='/tmp/cowd-wrong-logs')
        command = 'source "$1"; printf "%s\\n" "$CARGO_HOME" "$CARGO_TARGET_DIR" "$SCCACHE_DIR" "$COWD_GATE_LOG_DIR"'
        result = subprocess.run(['bash', '-c', command, 'test', str(SCRIPTS / 'build-env.sh')],
                                env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), [str(self.root / 'build' / p) for p in
                         ('cargo-home', 'leg-target', 'sccache', 'gate-logs')])
        self.assertEqual(result.stderr.count('outside build root'), 3)

if __name__ == '__main__':
    unittest.main()
