#!/usr/bin/env python3
"""Disposable metadata/Git/Cargo probes; no product build or external service."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
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
            manifest.write_text(f'[package]\nname = "{package["name"]}"\nversion = "1.2.3"\nedition = "2021"\n[features]\ntest-support = []\n[dependencies]\n{dependencies}')
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


if __name__ == "__main__":
    unittest.main()
