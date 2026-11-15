import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / 'scripts/test/bucket1-remeasure.py'

class BucketRunnerTest(unittest.TestCase):
    def run_case(self, log):
        with tempfile.TemporaryDirectory(dir=ROOT / 'target-gatefix') as tmp:
            root = Path(tmp)
            repo = root / 'repo'
            repo.mkdir()
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '--allow-empty', '-qm', 'fixture'], check=True)
            (root / 'cargo').write_text('#!/bin/sh\ncat <<\'EOF\'\n' + log + '\nEOF\n')
            (root / 'cargo').chmod(0o755)
            (root / 'rows.tsv').write_text('name\ttarget\nexample\texample\n')
            (root / 'metadata.json').write_text(json.dumps({'packages': [{'name': 'example', 'targets': [{'name': 'example', 'kind': ['lib']}]}]}))
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'])
            proc = subprocess.run(['python3', str(RUNNER), '--root', str(repo), '--out', str(root / 'out'), '--rows', str(root / 'rows.tsv'), '--metadata', str(root / 'metadata.json')], env=env, capture_output=True, text=True)
            summary = json.loads((root / 'out/summary.json').read_text())
            return proc.returncode, summary

    def test_skip_marker_cannot_pass(self):
        rc, summary = self.run_case('running 1 test\ntest example ... COWD_TEST_SKIPPED: fixture absent\nok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;')
        self.assertNotEqual(rc, 0)
        self.assertEqual((summary['pass'], summary['skip']), (0, 1))

    def test_zero_selected_cannot_pass(self):
        rc, summary = self.run_case('running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out;')
        self.assertNotEqual(rc, 0)
        self.assertEqual((summary['pass'], summary['invalid']), (0, 1))

    def test_real_result_can_pass(self):
        rc, summary = self.run_case('running 1 test\ntest example ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;')
        self.assertEqual(rc, 0)
        self.assertEqual((summary['pass'], summary['skip'], summary['invalid']), (1, 0, 0))

if __name__ == '__main__':
    unittest.main()
