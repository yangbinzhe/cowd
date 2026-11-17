#!/usr/bin/env python3
"""Negative cases for the local preflight unlock gates."""
import os
import os.path
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent
UNBLOCK = SCRIPTS / 'unblock.sh'


class EnvironmentUnblockTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cowd-environment-unblock-', dir='/dev/shm')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = dict(os.environ)

    def run_gate(self, *args):
        return subprocess.run(['bash', str(UNBLOCK), *map(str, args)],
                              env=self.env, text=True, capture_output=True)

    def test_disk_unlocks_a_dead_owned_pilot_root(self):
        pilot = self.root / 'pilot'
        dead = pilot / 'cowd-pilot-disk-case'
        dead.mkdir(parents=True)
        (dead / '.cowd-pilot-owner.json').write_text('{"pid": 1, "starttime": "stale"}')
        (dead / 'recoverable').write_text('dead-owned')
        (dead / '.cowd-pilot.lock').open('x').close()
        evidence = self.root / 'evidence'
        self.env.update(COWD_PILOT_ROOT=str(pilot), COWD_EVIDENCE_ROOT=str(evidence))
        result = self.run_gate('disk')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(dead.exists())
        self.assertEqual(len(list(evidence.glob('cowd-pilot-disk-case-*.tar'))), 1)

    def test_pg_unlock_requires_a_retained_closeout_receipt(self):
        report = self.root / 'owner-report.json'
        report.write_text('{}')
        result = self.run_gate('pg', report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('closeout receipt', result.stderr)

    def test_tmpfs_repairs_private_parent_mode(self):
        parent = self.root / 'tmpfs'
        parent.mkdir(mode=0o755)
        parent.chmod(0o755)
        self.env['COWD_TEST_PRIVATE_PARENT'] = str(parent)
        result = self.run_gate('tmpfs')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((parent.stat().st_mode & 0o777), 0o700)

    def test_lock_unlocks_a_stale_owned_file(self):
        lock = self.root / 'stale.lock'
        lock.open('x').close()
        lock.chmod(0o755)
        result = self.run_gate('lock', lock)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((lock.stat().st_mode & 0o777), 0o600)


if __name__ == '__main__':
    unittest.main()
