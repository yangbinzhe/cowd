#!/usr/bin/env python3
"""Disposable metadata/Git/Cargo probes; no product build or external service."""

import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("selection", Path(__file__).with_name("cargo-selection.py"))
selection = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(selection)


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="n7c-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "source"
        self.root.mkdir()
        self.evidence = Path(self.temp.name) / "evidence"
        self.evidence.mkdir()
        self.run_git("init", "-q")
        self.run_git("config", "user.name", "N7C Fixture")
        self.run_git("config", "user.email", "n7c@example.invalid")
        self.data = {"workspace_members": [], "packages": []}
        for name, dependencies in [("leaf", []), ("owner", ["leaf"]), ("consumer", ["owner"]), ("unrelated", [])]:
            self.add_package(name, dependencies)
        (self.root / "Cargo.toml").write_text("[workspace]\n")
        inventory = self.root / "tests/test-governance/test-inventory.yaml"
        inventory.parent.mkdir(parents=True)
        inventory.write_text("ignored_tests:\n  pg:\n    marker: COWD_TEST_POSTGRES_URL\n    runner: scripts/validate.sh manual postgres-contract\n")
        self.run_git("add", ".")
        self.run_git("commit", "-qm", "fixture")
        environment = dict(os.environ)
        for name in ["COWD_TEST_POSTGRES_URL", "COWD_TEST_LAUNCHER_IDENTITY", "COWD_SANDBOX_LAUNCHER_BINARY"]:
            environment.pop(name, None)
        self.env = patch.dict(os.environ, environment, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def run_git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root)

    def add_package(self, name, dependencies=(), **target_fields):
        directory = self.root / "crates" / name
        directory.mkdir(parents=True)
        (directory / "Cargo.toml").write_text("[package]\n")
        (directory / "src").mkdir()
        (directory / "src/lib.rs").write_text("#[test] fn works() {}\n")
        package = {"id": name, "name": name, "version": "1.2.3", "manifest_path": str(directory / "Cargo.toml"),
                   "dependencies": [{"name": dependency, "path": str(self.root / "crates" / dependency),
                                     "kind": "dev", "optional": True} for dependency in dependencies],
                   "targets": [{"name": name, "kind": ["lib"], "required-features": [], **target_fields}]}
        self.data["packages"].append(package)
        self.data["workspace_members"].append(name)
        return package

    def plan(self, *paths):
        return selection.make_plan(self.root, self.data, list(paths or ["crates/owner/src/lib.rs"]))

    def narrow(self, **overrides):
        request = dict(package="owner", lib=True, test=None, filter="module::works", all_in_target=False,
                       release=False, exact=True, ignored=False, nocapture=False, test_threads=None, entry=None)
        request.update(overrides)
        return selection.make_narrow_plan(self.root, self.data, request)

    def mixed_cargo(self, corrupt=None, registry=None):
        """Cargo double independently applies target/profile/filter/ignored semantics."""
        original = selection.run_process
        calls = []
        registry = registry if registry is not None else {"module::works": False, "module::other": False, "module::database": True}

        def run(args, **kwargs):
            if args[0] in {"git", "rustc"}:
                return original(args, **kwargs)
            calls.append(list(args))
            self.assertEqual(args[:3], ["cargo", "test", "--locked"])
            self.assertNotIn("--all-targets", args)
            self.assertNotIn("--all-features", args)
            self.assertNotIn("--no-default-features", args)
            name = args[args.index("-p") + 1]
            package = next(p for p in self.data["packages"] if p["name"] == name)
            target = next(t for t in package["targets"] if
                          ("--lib" in args and "lib" in t["kind"]) or
                          ("--test" in args and t["name"] == args[args.index("--test") + 1]))
            if "--no-run" in args:
                binary = self.evidence / (name + '-' + target['name'] + ('-release' if '--release' in args else '-debug'))
                binary.write_bytes(b"mixed Cargo fixture")
                artifact = {"reason": "compiler-artifact", "package_id": package["id"], "target": target,
                            "profile": {"test": True}, "features": [], "executable": str(binary)}
                return subprocess.CompletedProcess(args, 0, json.dumps(artifact).encode(), b"")
            harness = args[args.index("--") + 1:]
            filter_text = harness[0] if harness and not harness[0].startswith('--') else None
            names = [n for n in registry if filter_text is None or
                     (n == filter_text if '--exact' in harness else filter_text in n)]
            if '--ignored' in harness:
                names = [n for n in names if registry[n]]
            if '--list' in harness:
                return subprocess.CompletedProcess(args, 0, ''.join(n+': test\n' for n in names).encode(), b'')
            skipped = [n for n in names if registry[n] and '--ignored' not in harness]
            executed = [n for n in names if n not in skipped]
            if corrupt == 'names':
                executed = ['module::substituted' for n in executed]
            if corrupt == 'mode':
                executed, skipped = [], executed
            passed = len(executed) + (1 if corrupt == 'summary' else 0)
            lines = [f'test {n} ... ok\n' for n in executed] + [f'test {n} ... ignored\n' for n in skipped]
            output = ''.join(lines) + f'\ntest result: ok. {passed} passed; 0 failed; {len(skipped)} ignored;\n'
            return subprocess.CompletedProcess(args, 17 if corrupt == 'exit' else 0, output.encode(), b'')

        return patch.object(selection, 'run_process', side_effect=run), calls

    def register_mixed_ignored(self):
        (self.root / 'crates/owner/src/lib.rs').write_text(
            '#[test]\n#[ignore = "requires COWD_TEST_POSTGRES_URL"]\nfn database() {}\n')
        os.environ['COWD_TEST_POSTGRES_URL'] = 'postgresql://isolated.invalid/mock-only'

    def test_narrow_factory_exact_package_target_and_conservative_resources(self):
        self.add_package('session-postgres')
        self.data['packages'][1]['dependencies'].append({'path': str(self.root/'crates/session-postgres')})
        self.data['packages'][1]['targets'].append({'name': 'black_box', 'kind': ['test']})
        plan = self.narrow(lib=False, test='black_box', release=True)
        self.assertEqual(plan['packages'], ['owner'])
        self.assertEqual(plan['direct_packages'], ['owner'])
        self.assertEqual(plan['external_checks'], [])
        item, = plan['targets']
        self.assertEqual(item['target_args'], ['--test', 'black_box'])
        self.assertEqual(item['features'], 'default')
        self.assertTrue(item['needs_pg'])
        with self.assertRaisesRegex(selection.NotStarted, 'missing isolated PG'):
            selection.check_resources(self.root, plan, self.data)
        self.add_package('sandbox-launcher')
        self.data['packages'][1]['dependencies'].append({'path': str(self.root/'crates/sandbox-launcher')})
        os.environ['COWD_TEST_POSTGRES_URL'] = 'fixture-only'
        with self.assertRaisesRegex(selection.NotStarted, 'missing launcher'):
            selection.check_resources(self.root, self.narrow(), self.data)

    def test_invalid_narrow_targets_modes_and_entry_rejected(self):
        for change in [dict(package='missing'), dict(lib=False), dict(lib=False,test='absent'),
                       dict(test='also-lib'), dict(filter=None), dict(filter=' '),
                       dict(filter=None, exact=True, all_in_target=True), dict(filter=None,all_in_target=True),
                       dict(filter='--skip=module::works',exact=False),
                       dict(test_threads=0), dict(ignored=True), dict(entry='unregistered')]:
            with self.subTest(change=change), self.assertRaises(selection.NotStarted):
                self.narrow(**change)
        self.data['packages'][1]['targets'][0]['test'] = False
        with self.assertRaisesRegex(selection.NotStarted, 'executable libtest'):
            self.narrow()

    def test_narrow_build_list_run_share_default_release_and_filter_argv(self):
        for release in [False, True]:
            with self.subTest(release=release):
                log = self.evidence / str(release)
                log.mkdir()
                plan = self.narrow(release=release, nocapture=True, test_threads=1)
                mock, calls = self.mixed_cargo()
                with mock:
                    selection.discover(self.root, plan, self.data, log)
                    self.assertEqual(selection.execute(self.root, plan, self.data, log), 0)
                self.assertEqual(len(calls), 4)
                self.assertEqual(sum('--no-run' in args for args in calls), 1)
                for args in calls:
                    self.assertEqual('--release' in args, release)
                    self.assertIn('--lib', args)
                    self.assertEqual(args[args.index('-p')+1], 'owner')
                for args in calls[1:]:
                    self.assertIn('module::works', args)
                    self.assertIn('--exact', args)
                self.assertIn('--nocapture', calls[-1])
                self.assertEqual(calls[-1][-2:], ['--test-threads', '1'])
                self.assertEqual(plan['targets'][0]['selectors'], ['module::works'])

    def test_narrow_filtered_normal_and_ignored_sets(self):
        self.register_mixed_ignored()
        for ignored in [False, True]:
            with self.subTest(ignored=ignored):
                log=self.evidence/str(ignored); log.mkdir()
                plan=self.narrow(filter='module::', exact=False, ignored=ignored,
                                 entry='scripts/validate.sh manual postgres-contract' if ignored else None)
                mock, calls=self.mixed_cargo()
                with mock:
                    selection.discover(self.root,plan,self.data,log)
                    self.assertEqual(selection.execute(self.root,plan,self.data,log),0)
                item,=plan['targets']
                self.assertEqual(item['selectors'], ['module::database'] if ignored else ['module::other','module::works'])
                self.assertEqual(item['expected_skipped'], [] if ignored else ['module::database'])
                self.assertEqual('--ignored' in calls[-1],ignored)

    def test_narrow_all_in_named_target_is_one_invocation_not_four_names(self):
        self.register_mixed_ignored()
        self.data['packages'][1]['targets'].append({'name':'black_box','kind':['test']})
        plan=self.narrow(lib=False,test='black_box',filter=None,exact=False,all_in_target=True,
                         ignored=True,entry='scripts/validate.sh manual postgres-contract')
        mock,calls=self.mixed_cargo()
        with mock:
            selection.discover(self.root,plan,self.data,self.evidence)
            self.assertEqual(selection.execute(self.root,plan,self.data,self.evidence),0)
        self.assertEqual(len(calls),4)
        self.assertTrue(all(args[args.index('--test')+1]=='black_box' for args in calls))
        self.assertEqual(calls[-1][calls[-1].index('--')+1:],["--ignored","--format","pretty"])

    def test_narrow_wrong_ignored_mode_zero_and_unqualified_exact_rejected(self):
        self.register_mixed_ignored()
        for change in [dict(filter='absent'),dict(filter='works'),
                       dict(filter='module::database'),
                       dict(ignored=True,entry='scripts/validate.sh manual postgres-contract')]:
            with self.subTest(change=change):
                log=self.evidence/str(len(list(self.evidence.iterdir())));log.mkdir()
                mock,_=self.mixed_cargo()
                with mock,self.assertRaises(selection.NotStarted):
                    selection.discover(self.root,self.narrow(**change),self.data,log)

    def test_narrow_same_count_wrong_names_mode_summary_and_nonzero_rejected(self):
        for corrupt in ['names','mode','summary','exit']:
            with self.subTest(corrupt=corrupt):
                log=self.evidence/corrupt;log.mkdir()
                plan=self.narrow();mock,_=self.mixed_cargo(corrupt)
                with mock:
                    selection.discover(self.root,plan,self.data,log)
                    if corrupt=='exit':
                        self.assertEqual(selection.execute(self.root,plan,self.data,log),17)
                    else:
                        with self.assertRaises(selection.NotStarted):
                            selection.execute(self.root,plan,self.data,log)

    def test_narrow_canonical_owner_live_gate_and_lazy_reason_scope(self):
        inventory=self.root/'tests/test-governance/test-inventory.yaml'
        with inventory.open('a') as f:
            f.write('  live:\n    marker: COWD_AI_HARNESS_LIVE=1\n    runner: scripts/validate.sh manual live-provider\n')
        with self.assertRaisesRegex(selection.NotStarted,'LIVE opt-in'):
            self.narrow(ignored=True,entry='scripts/validate.sh manual live-provider')
        os.environ['COWD_AI_HARNESS_LIVE']='1'
        self.register_mixed_ignored()
        plan=self.narrow(filter='module::database',ignored=True,entry='scripts/validate.sh manual live-provider')
        mock,_=self.mixed_cargo()
        with mock,self.assertRaisesRegex(selection.NotStarted,'different canonical'):
            selection.discover(self.root,plan,self.data,self.evidence)
        plan=self.narrow()
        mock,_=self.mixed_cargo()
        with mock,patch.object(selection,'ignored_reasons',wraps=selection.ignored_reasons) as reasons:
            selection.discover(self.root,plan,self.data,self.evidence)
        reasons.assert_not_called()

    def test_empty_or_zero_narrow_cli_arguments_never_fall_back_to_wide(self):
        for options in [['--filter',''],['--test',''],['--test-threads','0'],['--entry','']]:
            with self.subTest(options=options),patch.object(sys,'argv',['selection','run',*options]),\
                 patch.object(selection,'metadata') as metadata,patch('sys.stderr',new_callable=io.StringIO):
                with self.assertRaises(SystemExit) as caught:selection.main()
                self.assertEqual(caught.exception.code,2)
                metadata.assert_not_called()

    def test_explicit_empty_wide_base_is_not_silently_replaced_with_head(self):
        with patch.object(sys,'argv',['selection','plan','--base','']),\
             patch.object(selection,'metadata',return_value=self.data),\
             patch.object(selection,'changed_paths',side_effect=selection.NotStarted('invalid empty base')) as changed,\
             patch('sys.stdout',new_callable=io.StringIO):
            self.assertEqual(selection.main(),2)
        self.assertEqual(changed.call_args.args[1],'')

    def test_same_leaf_cannot_borrow_another_canonical_owner_or_live_admission(self):
        self.register_mixed_ignored()
        path=self.root/'crates/owner/src/lib.rs'
        path.write_text('''mod a { #[test] #[ignore="requires COWD_TEST_POSTGRES_URL"] fn database() {} }
mod b { #[test] #[ignore="COWD_AI_HARNESS_LIVE=1"] fn database() {} }
''')
        with (self.root/'tests/test-governance/test-inventory.yaml').open('a') as f:
            f.write('  live:\n    marker: COWD_AI_HARNESS_LIVE=1\n    runner: scripts/validate.sh manual live-provider\n')
        plan=self.narrow(filter='b::database',ignored=True,entry='scripts/validate.sh manual postgres-contract')
        mock,calls=self.mixed_cargo(registry={'a::database':True,'b::database':True})
        with mock,self.assertRaisesRegex(selection.NotStarted,'ambiguous ignored reason'):
            selection.discover(self.root,plan,self.data,self.evidence)
        self.assertEqual(sum('--no-run' not in args and '--list' not in args for args in calls),0)

    def mock_cargo(self, zero=False, missing_features=False, unknown_ignore=False, changed_names=False, exit_code=0, all_ignored=False):
        def run(args, **kwargs):
            if args[0] in {"git", "rustc"}:
                return self.original_run(args, **kwargs)
            if "--no-run" in args:
                selected = {args[index + 1] for index, arg in enumerate(args) if arg == "-p"}
                artifacts = []
                for package in self.data["packages"]:
                    if package["name"] not in selected:
                        continue
                    for target in package["targets"]:
                        binary = self.evidence / (package["name"] + "-" + target["name"] + "-binary")
                        binary.write_bytes(b"binary fixture")
                        artifacts.append({"reason": "compiler-artifact", "package_id": package["id"],
                                          "target": {"name": target["name"], "kind": target["kind"]}, "profile": {"test": True},
                                          "features": [] if missing_features else ["test-support"], "executable": str(binary)})
                return subprocess.CompletedProcess(args, 0, b"\n".join(json.dumps(item).encode() for item in artifacts), b"")
            if "--list" in args:
                names = b"" if all_ignored else b"module::works: test\n"
                if unknown_ignore:
                    names += b"module::unregistered: test\n"
                if "--ignored" in args:
                    names = b"module::unregistered: test\n" if unknown_ignore else b""
                return subprocess.CompletedProcess(args, 0, b"" if zero else names, b"")
            name = "wrong" if changed_names else "works"
            output = f"test module::{name} ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;\n".encode()
            return subprocess.CompletedProcess(args, exit_code, output, b"")
        self.original_run = selection.run_process
        return patch.object(selection, "run_process", side_effect=run)

    def test_transitive_optional_dev_consumers_not_unrelated(self):
        self.assertEqual(self.plan("crates/leaf/src/lib.rs")["packages"], ["consumer", "leaf", "owner"])

    def test_multiple_owners_union(self):
        self.assertEqual(self.plan("crates/owner/src/lib.rs", "crates/unrelated/src/lib.rs")["packages"], ["consumer", "owner", "unrelated"])

    def test_deleted_file_keeps_owner_and_consumers(self):
        (self.root / "crates/owner/src/lib.rs").unlink()
        paths = selection.changed_paths(self.root, "HEAD")
        self.assertEqual(paths, ["crates/owner/src/lib.rs"])
        self.assertEqual(self.plan(*paths)["packages"], ["consumer", "owner"])

    def test_rename_captures_both_old_and_new_owners(self):
        self.run_git("mv", "crates/owner/src/lib.rs", "crates/unrelated/src/moved file.rs")
        paths = selection.changed_paths(self.root, "HEAD")
        self.assertEqual(paths, ["crates/owner/src/lib.rs", "crates/unrelated/src/moved file.rs"])
        self.assertEqual(self.plan(*paths)["packages"], ["consumer", "owner", "unrelated"])

    def test_invalid_base_fails_closed(self):
        with self.assertRaises(selection.NotStarted):
            selection.changed_paths(self.root, "does-not-exist")

    def test_untracked_file_with_spaces(self):
        (self.root / "crates/leaf/src/new file.rs").write_text("fn sample() {}")
        self.assertEqual(selection.changed_paths(self.root, "HEAD"), ["crates/leaf/src/new file.rs"])

    def test_any_manifest_lock_config_or_unknown_path_escalates(self):
        for path in ["Cargo.toml", "Cargo.lock", "crates/owner/Cargo.toml", ".cargo/config.toml", "rust-toolchain.toml", "scripts/test/quick.sh", "tests/interactive/src/lib.rs"]:
            with self.subTest(path=path):
                self.assertEqual(len(self.plan(path)["packages"]), 4)

    def test_longest_package_directory_wins(self):
        self.add_package("owner/nested")
        matched, unmatched = selection.owners(self.root, selection.workspace_packages(self.data), ["crates/owner/nested/src/lib.rs"])
        self.assertEqual(matched, {"owner/nested"})
        self.assertEqual(unmatched, [])

    def test_metadata_required_target_and_features_preserved(self):
        self.data["packages"][1]["targets"].append({"name": "black_box", "kind": ["test"], "required-features": ["test-support"]})
        targets = self.plan()["targets"]
        entry = next(item for item in targets if item["target"] == "black_box")
        self.assertEqual(entry["target_args"], ["--test", "black_box"])
        self.assertEqual(entry["features"], "all")
        self.assertEqual(entry["required_features"], ["test-support"])

    def test_source_identity_changes_with_bytes_deletion_and_symlink(self):
        before = selection.source_identity(self.root)
        path = self.root / "crates/owner/src/lib.rs"
        path.write_text("changed")
        self.assertNotEqual(selection.source_identity(self.root), before)
        changed = selection.source_identity(self.root)
        path.unlink()
        self.assertNotEqual(selection.source_identity(self.root), changed)

    def test_stale_source_rejected(self):
        plan = self.plan()
        (self.root / "Cargo.toml").write_text("changed")
        with self.assertRaisesRegex(selection.NotStarted, "source changed"):
            selection.check_source(self.root, plan)

    def test_missing_pg_is_admission_failure(self):
        self.add_package("session-postgres")
        self.data["packages"][1]["dependencies"].append({"path": str(self.root / "crates/session-postgres")})
        with self.assertRaisesRegex(selection.NotStarted, "missing isolated PG"):
            selection.check_resources(self.root, self.plan(), self.data)

    def test_launcher_missing_and_wrong_hash_rejected(self):
        self.add_package("sandbox-launcher")
        self.data["packages"][1]["dependencies"].append({"path": str(self.root / "crates/sandbox-launcher")})
        plan = self.plan()
        with self.assertRaisesRegex(selection.NotStarted, "missing launcher"):
            selection.check_resources(self.root, plan, self.data)
        launcher = self.evidence / "launcher"
        launcher.write_text("fixture")
        receipt = self.evidence / "launcher.json"
        receipt.write_text(json.dumps({"source": plan["source"], "version": "1.2.3", "path": str(launcher), "sha256": "wrong"}))
        os.environ.update(COWD_TEST_LAUNCHER_IDENTITY=str(receipt), COWD_SANDBOX_LAUNCHER_BINARY=str(launcher))
        with self.assertRaisesRegex(selection.NotStarted, "mismatch"):
            selection.check_resources(self.root, plan, self.data)

    def test_discovery_and_execution_are_distinct_and_match(self):
        plan = self.plan()
        with self.mock_cargo():
            selection.discover(self.root, plan, self.data, self.evidence)
            self.assertEqual(plan["status"], "Discovered")
            self.assertEqual(plan["business_acceptance"], "NotAssessed")
            self.assertEqual(selection.execute(self.root, plan, self.data, self.evidence), 0)
        self.assertEqual(plan["status"], "TestsPassed")
        self.assertEqual([item["expected_selected"] for item in plan["targets"]], [1, 1])

    def test_zero_discovery_rejected(self):
        with self.mock_cargo(zero=True), self.assertRaisesRegex(selection.NotStarted, "zero selected"):
            selection.discover(self.root, self.plan(), self.data, self.evidence)

    def test_missing_required_feature_rejected(self):
        self.data["packages"][1]["targets"][0]["required-features"] = ["test-support"]
        with self.mock_cargo(missing_features=True), self.assertRaisesRegex(selection.NotStarted, "required features"):
            selection.discover(self.root, self.plan(), self.data, self.evidence)

    def test_missing_target_artifact_rejected(self):
        plan = self.plan()
        plan["targets"][0]["target"] = "absent"
        with self.mock_cargo(), self.assertRaisesRegex(selection.NotStarted, "exactly one"):
            selection.discover(self.root, plan, self.data, self.evidence)

    def test_unknown_ignored_reason_rejected(self):
        with self.mock_cargo(unknown_ignore=True), self.assertRaisesRegex(selection.NotStarted, "unknown ignored"):
            selection.discover(self.root, self.plan(), self.data, self.evidence)

    def test_same_count_different_test_is_rejected(self):
        plan = self.plan()
        with self.mock_cargo():
            selection.discover(self.root, plan, self.data, self.evidence)
        with self.mock_cargo(changed_names=True), self.assertRaisesRegex(selection.NotStarted, "selector set"):
            selection.execute(self.root, plan, self.data, self.evidence)

    def test_changed_binary_rejected_before_execution(self):
        plan = self.plan()
        with self.mock_cargo():
            selection.discover(self.root, plan, self.data, self.evidence)
        Path(plan["targets"][0]["binary"]).write_bytes(b"different")
        with self.assertRaisesRegex(selection.NotStarted, "binary changed"):
            selection.execute(self.root, plan, self.data, self.evidence)

    def test_between_target_artifact_replacement_rejected(self):
        self.data["packages"][1]["targets"].append({"name": "second", "kind": ["test"], "required-features": []})
        plan = self.plan()
        plan["targets"] = [item for item in plan["targets"] if item["package"] == "owner"]
        original_listing = selection.listed_output

        def replace_after_first_target(root, args, path):
            output = original_listing(root, args, path)
            if path.name == "0-ignored.txt":
                (self.evidence / "owner-second-binary").write_bytes(b"replacement between targets")
            return output

        with self.mock_cargo(), patch.object(selection, "listed_output", side_effect=replace_after_first_target):
            with self.assertRaisesRegex(selection.NotStarted, "binary changed"):
                selection.discover(self.root, plan, self.data, self.evidence)

    def test_host_probe_is_scoped_to_discover_and_preserves_failures(self):
        original_command = selection.command
        compiler = "/fixture/custom-rustc"
        responses = [b"host: fixture-native\n", b"host: next-native\n", b"missing host\n"]

        def command(root, args, **kwargs):
            if args[0] == compiler:
                self.assertEqual(args, [compiler, "-vV"])
                return responses.pop(0)
            return original_command(root, args, **kwargs)

        with self.mock_cargo(), patch.dict(os.environ, RUSTC=compiler), patch.object(selection, "command", side_effect=command):
            for host in ["fixture-native", "next-native"]:
                plan = self.plan()
                selection.discover(self.root, plan, self.data, self.evidence)
                self.assertTrue(all(f"target.{host}.runner=" in item["cargo_args"][-1] for item in plan["targets"]))
            with self.assertRaisesRegex(selection.NotStarted, "cannot determine native"):
                selection.discover(self.root, self.plan(), self.data, self.evidence)
        with patch.dict(os.environ, RUSTC=compiler), patch.object(selection, "run_process", return_value=subprocess.CompletedProcess([], 7, b"", b"")):
            with self.assertRaisesRegex(selection.NotStarted, "command failed .exit 7"):
                selection.native_host(self.root)

    def test_duplicate_discovered_selector_rejected(self):
        with self.assertRaisesRegex(selection.NotStarted, "duplicate"):
            selection.parse_listing(b"same: test\nsame: test\n")

    def test_registered_ignored_reason_retains_canonical_owner(self):
        path = self.root / "crates/owner/src/lib.rs"
        path.write_text('#[test]\n#[ignore = "requires COWD_TEST_POSTGRES_URL"]\nfn database() {}\n')
        reasons = selection.ignored_reasons(self.root, self.data)
        self.assertEqual(reasons["owner"]["database"], {"requires COWD_TEST_POSTGRES_URL"})

    def test_known_ignored_tests_keep_manual_owner_and_zero_total_rejects(self):
        for package in ["owner", "consumer"]:
            path = self.root / f"crates/{package}/src/lib.rs"
            path.write_text('#[test]\n#[ignore = "requires COWD_TEST_POSTGRES_URL"]\nfn unregistered() {}\n')
        plan = self.plan()
        with self.mock_cargo(unknown_ignore=True):
            selection.discover(self.root, plan, self.data, self.evidence)
        self.assertEqual(plan["targets"][0]["ignored_runners"], {"module::unregistered": ["scripts/validate.sh manual postgres-contract"]})
        plan = self.plan()
        with self.mock_cargo(unknown_ignore=True, all_ignored=True), self.assertRaisesRegex(selection.NotStarted, "zero selected"):
            selection.discover(self.root, plan, self.data, self.evidence)
        self.assertEqual(plan["targets"][0]["status"], "CanonicalManualOnly")

    def test_interactive_check_is_an_executable_obligation(self):
        self.assertEqual(self.plan("tests/interactive/src/lib.rs")["external_checks"], [["cargo", "check", "--locked", "--manifest-path", "tests/interactive/Cargo.toml", "--all-targets", "--all-features"]])

    def test_ordinary_binary_is_bound_even_when_harness_is_unchanged(self):
        plan = self.plan()
        plan["executables"] = {}
        for name in ["harness", "ordinary-cli"]:
            path = self.evidence / name
            path.write_bytes(name.encode())
            plan["executables"][str(path)] = selection.digest(path.read_bytes())
        selection.validate_executables(self.root, plan)
        (self.evidence / "ordinary-cli").write_bytes(b"changed CLI with unchanged harness")
        with self.assertRaisesRegex(selection.NotStarted, "binary changed"):
            selection.validate_executables(self.root, plan)

    def test_config_only_launcher_respects_force_and_relative(self):
        self.add_package("sandbox-launcher")
        self.data["packages"][1]["dependencies"].append({"path": str(self.root / "crates/sandbox-launcher")})
        launcher = self.evidence / "launcher"
        launcher.write_text("#!/bin/sh\nprintf 'sandbox-launcher/1.2.3/kernel-v2\\n'\n")
        launcher.chmod(0o700)
        receipt = self.evidence / "launcher.json"
        config = self.root / ".cargo/config.toml"
        config.parent.mkdir()
        config.write_text(f'[env]\nCOWD_SANDBOX_LAUNCHER_BINARY = {{ value = "../evidence/launcher", relative = true, force = true }}\nCOWD_TEST_LAUNCHER_IDENTITY = "{receipt}"\n')
        plan = self.plan()
        receipt.write_text(json.dumps({"source": plan["source"], "version": "1.2.3", "path": str(launcher), "sha256": selection.digest(launcher.read_bytes())}))
        os.environ["COWD_SANDBOX_LAUNCHER_BINARY"] = "wrong-inherited-path"
        selection.check_resources(self.root, plan, self.data)

    def test_logs_redact_secrets_and_connection_strings(self):
        os.environ["STEP_API_KEY"] = "fixture-private-token"
        os.environ["COWD_TEST_POSTGRES_URL"] = "postgresql://user:password@localhost/private"
        log = self.evidence / "secret.log"
        selection.write_log(log, b"fixture-private-token postgresql://user:password@localhost/private https://example.invalid/?token=abc")
        self.assertEqual(log.read_text(), "<redacted> <redacted> <redacted-url>")

    def test_config_only_token_is_redacted_in_each_log_kind(self):
        config = self.root / ".cargo/config.toml"
        config.parent.mkdir()
        config.write_text('[env]\nSTEP_API_KEY = { value = "config-only-secret-value", force = true }\n')
        os.environ["STEP_API_KEY"] = "inherited-secret-value"
        for kind in ["build.jsonl", "list.txt", "run.stdout", "run.stderr"]:
            path = self.evidence / kind
            selection.write_log(path, b"config-only-secret-value inherited-secret-value", root=self.root)
            self.assertEqual(path.read_text(), "<redacted> <redacted>")

    def test_timeout_reaps_owned_parent_and_child(self):
        pidfile = self.evidence / "pids"
        script = '''import os, signal, subprocess, sys, time
from pathlib import Path
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
def stop(signum, frame):
    child.wait(timeout=2)
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
Path(sys.argv[1]).write_text(str(os.getpid()) + " " + str(child.pid))
print("started", flush=True)
time.sleep(60)
'''
        with self.assertRaises(subprocess.TimeoutExpired) as caught:
            selection.run_process([sys.executable, "-c", script, str(pidfile)], cwd=self.root, timeout=1)
        self.assertIn(b"started", caught.exception.stdout)
        for identity in pidfile.read_text().split():
            with self.assertRaises(ProcessLookupError):
                os.kill(int(identity), 0)

    def test_timeout_cleans_group_even_when_parent_closes_pipes(self):
        process = unittest.mock.MagicMock()
        process.pid = 987654
        process.communicate.side_effect = [subprocess.TimeoutExpired("owned cargo", 1), (b"", b""), (b"", b"")]
        process.__enter__.return_value = process
        with patch.object(subprocess, "Popen", return_value=process), patch.object(os, "killpg") as kill_group:
            with self.assertRaises(subprocess.TimeoutExpired):
                selection.run_process(["owned-cargo"], cwd=self.root, timeout=1)
        self.assertEqual([call.args for call in kill_group.call_args_list], [(987654, selection.signal.SIGTERM), (987654, selection.signal.SIGKILL)])

    def test_timeout_pipe_drain_and_reap_are_always_bounded(self):
        process=unittest.mock.MagicMock()
        process.pid=987655
        process.communicate.side_effect=subprocess.TimeoutExpired('owned fixture',1)
        with patch.object(subprocess,'Popen',return_value=process),patch.object(os,'killpg'):
            with self.assertRaises(subprocess.TimeoutExpired):
                selection.run_process(['owned-fixture'],cwd=self.root,timeout=1)
        self.assertEqual([c.kwargs['timeout'] for c in process.communicate.call_args_list],[1,2,2])
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()
        process.wait.assert_called_once_with(timeout=1)

    def test_spawn_boundary_cancel_defers_until_owned_process_is_published(self):
        original=subprocess.Popen
        for sig in [signal.SIGTERM,signal.SIGINT]:
            with self.subTest(sig=sig):
                children=[]
                def spawn(*args,**kwargs):
                    child=original(*args,**kwargs);children.append(child)
                    os.kill(os.getpid(),sig)
                    return child
                before={s:signal.getsignal(s) for s in [signal.SIGTERM,signal.SIGINT]}
                try:
                    with selection.cli_signals():
                        with patch.object(subprocess,'Popen',side_effect=spawn),self.assertRaises(selection.Cancelled):
                            selection.run_process([sys.executable,'-c','import time;time.sleep(60)'],cwd=self.root,timeout=5)
                    self.assertEqual(len(children),1)
                    self.assertIsNotNone(children[0].returncode)
                    with self.assertRaises(ProcessLookupError):os.kill(children[0].pid,0)
                    self.assertEqual({s:signal.getsignal(s) for s in before},before)
                finally:
                    for child in children:
                        if child.poll() is None:
                            os.killpg(child.pid,signal.SIGKILL)
                            child.wait(timeout=2)
                        child.stdout.close();child.stderr.close()

    def test_timeout_escalates_uncooperative_owned_process(self):
        ready=self.evidence/'stubborn-ready'
        script='import os,signal,time,sys;from pathlib import Path;signal.signal(signal.SIGTERM,signal.SIG_IGN);Path(sys.argv[1]).write_text(str(os.getpid()));time.sleep(60)'
        start=time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            selection.run_process([sys.executable,'-c',script,str(ready)],cwd=self.root,timeout=1)
        self.assertTrue(ready.exists())
        self.assertLess(time.monotonic()-start,6)
        with self.assertRaises(ProcessLookupError):os.kill(int(ready.read_text()),0)

    def test_cli_term_and_int_reap_owned_cargo_and_child_and_restore_handlers(self):
        helper_dir=self.root/'scripts/test';helper_dir.mkdir(parents=True)
        helper=helper_dir/'cargo-selection.py'
        shutil.copyfile(Path(__file__).with_name('cargo-selection.py'),helper)
        probes=self.evidence/'bin';probes.mkdir()
        cargo=probes/'cargo'
        cargo.write_text(f'#!{sys.executable}\n'+'''import os,signal,subprocess,sys,time
from pathlib import Path
child=subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"])
def stop(sig,frame):
    child.wait(timeout=2)
    raise SystemExit(0)
signal.signal(signal.SIGTERM,stop)
Path(os.environ['OWNED_PIDFILE']).write_text(str(os.getpid())+' '+str(child.pid))
time.sleep(60)
''');cargo.chmod(0o700)
        for sig in [signal.SIGTERM,signal.SIGINT]:
            with self.subTest(sig=sig):
                pidfile=self.evidence/f'pids-{sig}'
                env={**os.environ,'PATH':str(probes)+os.pathsep+os.environ['PATH'],'OWNED_PIDFILE':str(pidfile)}
                process=subprocess.Popen([sys.executable,str(helper),'plan','--package','owner','--lib','--filter','works'],
                                         cwd=self.root,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
                try:
                    deadline=time.monotonic()+5
                    while not pidfile.exists() and time.monotonic()<deadline and process.poll() is None:
                        time.sleep(0.01)
                    self.assertTrue(pidfile.exists())
                    process.send_signal(sig)
                    out,err=process.communicate(timeout=7)
                    self.assertEqual(process.returncode,2,(out,err))
                    self.assertIn('cancelled by signal',json.loads(out)['reason'])
                    for identity in pidfile.read_text().split():
                        with self.assertRaises(ProcessLookupError):os.kill(int(identity),0)
                finally:
                    if process.poll() is None:
                        process.send_signal(signal.SIGTERM)
                        try:process.communicate(timeout=7)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid,signal.SIGKILL)
                            process.communicate(timeout=2)
                    if pidfile.exists():
                        try:os.killpg(int(pidfile.read_text().split()[0]),signal.SIGKILL)
                        except ProcessLookupError:pass
        before={sig:signal.getsignal(sig) for sig in [signal.SIGTERM,signal.SIGINT]}
        with selection.cli_signals():
            self.assertTrue(all(signal.getsignal(sig)!=previous for sig,previous in before.items()))
        self.assertEqual({sig:signal.getsignal(sig) for sig in before},before)

    def test_launcher_protocol_source_version_and_hash(self):
        self.add_package("sandbox-launcher")
        self.data["packages"][1]["dependencies"].append({"path": str(self.root / "crates/sandbox-launcher")})
        plan = self.plan()
        launcher = self.evidence / "launcher"
        launcher.write_text("#!/bin/sh\nprintf 'sandbox-launcher/1.2.3/kernel-v2\\n'\n")
        launcher.chmod(0o700)
        identity = {"source": plan["source"], "version": "1.2.3", "path": str(launcher),
                    "sha256": selection.digest(launcher.read_bytes())}
        receipt = self.evidence / "launcher.json"
        os.environ.update(COWD_TEST_LAUNCHER_IDENTITY=str(receipt), COWD_SANDBOX_LAUNCHER_BINARY=str(launcher))
        receipt.write_text(json.dumps(identity))
        selection.check_resources(self.root, plan, self.data)
        for field in ["source", "version", "path", "sha256"]:
            with self.subTest(field=field):
                receipt.write_text(json.dumps({**identity, field: "wrong"}))
                with self.assertRaisesRegex(selection.NotStarted, "mismatch"):
                    selection.check_resources(self.root, plan, self.data)
        launcher.write_text("#!/bin/sh\nprintf 'old-protocol\\n'\n")
        receipt.write_text(json.dumps({**identity, "sha256": selection.digest(launcher.read_bytes())}))
        with self.assertRaisesRegex(selection.NotStarted, "protocol"):
            selection.check_resources(self.root, plan, self.data)

    def test_real_disposable_cargo_workspace_and_stale_plan(self):
        helper_dir = self.root / "scripts/test"
        helper_dir.mkdir(parents=True)
        helper = helper_dir / "cargo-selection.py"
        shutil.copyfile(Path(__file__).with_name("cargo-selection.py"), helper)
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/*"]\nresolver = "2"\n')
        for package in self.data["packages"]:
            manifest = Path(package["manifest_path"])
            dependencies = "".join(f'{dep["name"]} = {{path = "../{dep["name"]}", optional = true}}\n' for dep in package["dependencies"])
            manifest.write_text(f'[package]\nname = "{package["name"]}"\nversion = "1.2.3"\nedition = "2021"\n[features]\ndefault = ["fixture-default"]\nfixture-default = []\ntest-support = []\n[dependencies]\n{dependencies}')
        target = self.root / "crates/owner/tests"
        target.mkdir()
        (target / "gated.rs").write_text('''#[test] fn gated() {
    assert_eq!(std::env::var("CARGO_PKG_NAME").unwrap(), "owner");
    assert_eq!(std::env::current_dir().unwrap().to_str().unwrap(), env!("CARGO_MANIFEST_DIR"));
    assert_eq!(std::env::var("CARGO_PKG_VERSION").unwrap(), "1.2.3");
    assert_eq!(std::env::var("FROM_BUILD").unwrap(), "owner-build-value");
    assert_eq!(std::env::var("FROM_CONFIG").unwrap(), "forced-config-value");
    assert!(std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).is_dir());
}
''')
        (self.root / "crates/owner/build.rs").write_text('fn main() { println!("cargo:rustc-env=FROM_BUILD=owner-build-value"); }\n')
        config = self.root / ".cargo/config.toml"
        config.parent.mkdir()
        config.write_text('[env]\nFROM_CONFIG = { value = "forced-config-value", force = true }\n')
        os.environ["FROM_CONFIG"] = "wrong-inherited-value"
        examples = self.root / "crates/owner/examples"
        examples.mkdir()
        (examples / "compile_only.rs").write_text("fn main() {}\n")
        binaries = self.root / "crates/owner/src/bin"
        binaries.mkdir()
        (binaries / "empty.rs").write_text("fn main() {}\n")
        with (self.root / "crates/owner/Cargo.toml").open("a") as manifest:
            manifest.write('\n[[test]]\nname = "gated"\nrequired-features = ["test-support"]\n')
        os.environ.update(CARGO_TARGET_DIR=str(self.evidence / "target"), CARGO_BUILD_JOBS="2", CARGO_INCREMENTAL="0")
        self.assertEqual(subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=self.root, capture_output=True).returncode, 0)
        actual_cargo = shutil.which("cargo")
        probes = self.evidence / "bin"
        probes.mkdir()
        trace = self.evidence / "cargo-calls.jsonl"
        wrapper = probes / "cargo"
        wrapper.write_text(f'#!{sys.executable}\nimport json, os, sys\nwith open({str(trace)!r}, "a") as output:\n    output.write(json.dumps(sys.argv[1:]) + "\\n")\nos.execv({actual_cargo!r}, [{actual_cargo!r}, *sys.argv[1:]])\n')
        wrapper.chmod(0o700)
        os.environ["PATH"] = str(probes) + os.pathsep + os.environ["PATH"]
        self.run_git("add", ".")
        self.run_git("commit", "-qm", "real fixture")
        report = self.evidence / "real-run.json"
        result = subprocess.run([sys.executable, str(helper), "run", "crates/owner/src/lib.rs", "--output", str(report)], capture_output=True, timeout=60)
        record = json.loads(report.read_text())
        self.assertEqual(result.returncode, 0, record)
        self.assertEqual(record["packages"], ["consumer", "owner"])
        self.assertEqual({(item["package"], item["target"]) for item in record["targets"]}, {("consumer", "consumer"), ("owner", "owner"), ("owner", "gated"), ("owner", "compile_only"), ("owner", "empty")})
        self.assertEqual(record["status"], "TestsPassed")
        self.assertEqual(record["business_acceptance"], "NotAssessed")
        self.assertEqual(sum(item["expected_selected"] for item in record["targets"]), 3)
        self.assertTrue(all(item["status"] == "CompiledOnly" for item in record["targets"] if item["target"] in {"compile_only", "empty"}))
        calls = [json.loads(line) for line in trace.read_text().splitlines()]
        self.assertEqual(sum("--no-run" in args for args in calls), 2)
        if os.environ.get("COWD_N7C_EVIDENCE_DIR"):
            destination = Path(os.environ["COWD_N7C_EVIDENCE_DIR"])
            destination.mkdir(parents=True, exist_ok=False)
            shutil.copyfile(report, destination / "report.json")
            shutil.copyfile(trace, destination / "cargo-calls.jsonl")
            shutil.copytree(report.with_suffix(".logs"), destination / "logs")
        planned = self.evidence / "plan.json"
        result = subprocess.run([sys.executable, str(helper), "plan", "crates/owner/src/lib.rs", "--output", str(planned)], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout)
        (self.root / "crates/owner/src/lib.rs").write_text("changed source")
        result = subprocess.run([sys.executable, str(helper), "run", "crates/owner/src/lib.rs", "--plan", str(planned), "--output", str(self.evidence / "stale.json")], capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("stale", (self.evidence / "stale.json").read_text())
        old_bytes = report.read_bytes()
        result = subprocess.run([sys.executable, str(helper), "plan", "--output", str(report)], capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(report.read_bytes(), old_bytes)
        result = subprocess.run([sys.executable, str(helper), "report"], capture_output=True)
        self.assertEqual(result.returncode, 0)
        self.assertFalse(Path(result.stdout.decode().strip()).is_relative_to(self.root))

        # Same tiny workspace, now independently sensitive to default features,
        # profile and ignored/filter semantics. An unrelated target must not build.
        fixture_marker='scripts/test/fixture-ignored.sh'
        (self.root/fixture_marker).write_text('# fixture canonical owner; never executed\n')
        with (self.root/'tests/test-governance/test-inventory.yaml').open('a') as inventory:
            inventory.write(f'  fixture:\n    marker: {fixture_marker}\n    runner: {fixture_marker}\n')
        probe='''fn verify() {
    assert!(!cfg!(feature="test-support"));
    assert!(cfg!(feature="fixture-default"));
    assert_eq!(cfg!(debug_assertions), std::env::var("FIXTURE_DEBUG").unwrap()=="true");
    assert_eq!(std::env::var("FROM_BUILD").unwrap(), "owner-build-value");
    assert_eq!(std::env::var("FROM_CONFIG").unwrap(), "forced-config-value");
    assert_eq!(std::env::current_dir().unwrap().to_str().unwrap(), env!("CARGO_MANIFEST_DIR"));
    assert!(std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).is_dir());
}
mod module {
    #[test] fn works() { super::verify(); }
    #[test] #[ignore="scripts/test/fixture-ignored.sh"]
    fn manual() { super::verify(); std::fs::write(std::env::var("MANUAL_MARKER").unwrap(), "ran").unwrap(); }
}
'''
        (self.root/'crates/owner/src/lib.rs').write_text(probe)
        (target/'narrow_probe.rs').write_text(probe)
        (target/'unselected.rs').write_text('compile_error!("unselected integration must not build");')
        manual_marker=self.evidence/'manual-ran'
        os.environ['MANUAL_MARKER']=str(manual_marker)
        for label,options,debug,expected in [
            ('narrow-lib',['--lib','--filter','module::works','--exact'],True,['module::works']),
            ('narrow-test',['--test','narrow_probe','--filter','module::works','--exact'],True,['module::works']),
            ('narrow-ignored',['--test','narrow_probe','--release','--filter','module::manual','--exact','--ignored','--entry',fixture_marker],False,['module::manual']),
            ('narrow-all',['--test','narrow_probe','--release','--all-in-target','--ignored','--entry',fixture_marker],False,['module::manual'])]:
            with self.subTest(label=label):
                report=self.evidence/(label+'.json')
                with patch.dict(os.environ,FIXTURE_DEBUG='true' if debug else 'false'):
                    result=selection.run_process([sys.executable,str(helper),'run','--package','owner',*options,
                                                  '--test-threads','1','--nocapture','--output',str(report)],cwd=self.root,timeout=40)
                record=json.loads(report.read_text())
                self.assertEqual(result.returncode,0,(record,result.stderr))
                item,=record['targets']
                self.assertEqual(record['packages'],['owner'])
                self.assertEqual(item['features'],'default')
                self.assertEqual(item['selectors'],expected)
                self.assertNotIn('--all-targets',item['build_command'])
                self.assertNotIn('--all-features',item['build_command'])
                self.assertEqual('--release' in item['build_command'],not debug)
                self.assertEqual(manual_marker.exists(),not debug)
                if manual_marker.exists():self.assertEqual(manual_marker.read_text(),'ran')
                if os.environ.get('COWD_N7C_EVIDENCE_DIR'):
                    destination=Path(os.environ['COWD_N7C_EVIDENCE_DIR'])
                    shutil.copyfile(report,destination/report.name)
                    shutil.copytree(report.with_suffix('.logs'),destination/report.with_suffix('.logs').name)
        # CLI rejects mixed wide/narrow requests before invoking Cargo.
        calls_before=trace.read_bytes()
        result=selection.run_process([sys.executable,str(helper),'plan','--package','owner','--lib','--filter','works','--quick'],cwd=self.root,timeout=5)
        self.assertEqual(result.returncode,2)
        self.assertEqual(trace.read_bytes(),calls_before)
        planned=self.evidence/'narrow-plan.json'
        options=['--package','owner','--lib','--filter','module::works','--exact']
        result=selection.run_process([sys.executable,str(helper),'plan',*options,'--output',str(planned)],cwd=self.root,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)
        result=selection.run_process([sys.executable,str(helper),'run',*options,'--release','--plan',str(planned),
                                      '--output',str(self.evidence/'mismatched-request.json')],cwd=self.root,timeout=10)
        self.assertEqual(result.returncode,2)
        self.assertIn('stale',json.loads((self.evidence/'mismatched-request.json').read_text())['reason'])
        if os.environ.get('COWD_N7C_EVIDENCE_DIR'):
            destination=Path(os.environ['COWD_N7C_EVIDENCE_DIR'])
            shutil.copyfile(trace,destination/'all-cargo-calls.jsonl')
            shutil.copyfile(self.evidence/'mismatched-request.json',destination/'mismatched-request.json')


if __name__ == "__main__":
    unittest.main()
