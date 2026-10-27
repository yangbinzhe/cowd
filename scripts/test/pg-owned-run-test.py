#!/usr/bin/env python3
"""Deterministic outer-owner safety cases; real PG cleanup is separately run."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import signal
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("pg_owned", Path(__file__).with_name("pg-owned-run.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class OwnerBoundary(unittest.TestCase):
    def owner(self, directory):
        args = argparse.Namespace(run_id="unit", port=36435, seconds=1,
                                  report=Path(directory)/"report.json", command=["unused"])
        return module.Owner(args)

    def resources(self, owner):
        owner.report.update(container_attempted=True, volume_attempted=True,
                            labels=owner.labels, child_exit=101, status="failed")
        present = {"container", "volume"}
        calls = []
        def operation(name, argv, *args, **kwargs):
            calls.append(name)
            for kind, resource in (("container", owner.container), ("volume", owner.volume)):
                if name == "find-attempted-" + kind:
                    return resource if kind in present else ""
                if name == "inspect-" + kind:
                    return json.dumps([{"Config": {"Labels": owner.labels},
                                        "Labels": owner.labels, "State": {"Running": False}}])
                if name == "remove-" + kind:
                    present.remove(kind)
                    return ""
                if name == "verify-absent-" + kind:
                    return "" if kind not in present else resource
            if name == "stop-owned":
                return ""
            self.fail("unexpected operation: " + name)
        return operation, calls, present

    def receipt(self, owner, directory):
        evidence = Path(directory)/"revalidation.json"
        evidence.write_text('{"actual_assertions":"passed"}\n')
        contents = {"run_id": owner.args.run_id, "container": owner.container,
                    "volume": owner.volume, "owner_nonce": owner.nonce,
                    "kind": "issue_closure", "issue": "unit-defect-1",
                    "root_cause": "identified causal defect", "fix": "verified-source-revision",
                    "revalidation_passed": True,
                    "evidence": {"path": str(evidence), "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest()}}
        path = Path(directory)/"closeout.json"
        path.write_text(json.dumps(contents))
        path.chmod(0o600)
        owner.args.closeout_receipt = path
        return path, contents

    def test_failure_timeout_signal_and_unknown_acceptance_retain_volume(self):
        for mode in ("failure", "timeout", "signal", "exit_zero"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                owner = self.owner(directory)
                operation, calls, present = self.resources(owner)
                def child():
                    if mode == "timeout":
                        raise TimeoutError("actual child deadline")
                    if mode == "signal":
                        owner.interrupted = signal.SIGTERM
                    owner.report["child_exit"] = 0 if mode == "exit_zero" else 101
                with patch.object(owner, "start"), patch.object(owner, "run_child", side_effect=child), patch.object(owner, "op", side_effect=operation):
                    result = owner.execute()
                    if mode == "exit_zero":
                        self.assertEqual(result, 0)
                    else:
                        self.assertNotEqual(result, 0)
                self.assertEqual(present, {"container", "volume"})
                self.assertIn("stop-owned", calls)
                self.assertNotIn("remove-volume", calls)
                self.assertTrue(owner.report["volume_retained"])
                self.assertTrue(owner.report["port_bindable"])
                self.assertFalse(owner.report["deletion_authorized"])
                self.assertEqual(owner.report["status"], "acceptance_pending" if mode == "exit_zero" else "failed")

    def test_foreign_unresolved_incomplete_or_tampered_receipt_cannot_delete(self):
        cases = ("foreign_run", "foreign_owner", "foreign_volume", "missing_cause",
                 "missing_fix", "not_revalidated", "tampered_evidence", "incomplete_acceptance")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                owner = self.owner(directory)
                operation, calls, present = self.resources(owner)
                path, receipt = self.receipt(owner, directory)
                if case.startswith("foreign_"):
                    key = {"foreign_run": "run_id", "foreign_owner": "owner_nonce", "foreign_volume": "volume"}[case]
                    receipt[key] = "foreign"
                elif case == "missing_cause":
                    del receipt["root_cause"]
                elif case == "missing_fix":
                    receipt["fix"] = ""
                elif case == "not_revalidated":
                    receipt["revalidation_passed"] = False
                elif case == "tampered_evidence":
                    Path(receipt["evidence"]["path"]).write_text("changed")
                else:
                    receipt.update(kind="business_acceptance", business_success=True, acceptance_complete=False)
                    owner.report["child_exit"] = 0
                path.write_text(json.dumps(receipt))
                with patch.object(owner, "op", side_effect=operation):
                    owner.cleanup()
                self.assertEqual(present, {"container", "volume"})
                self.assertFalse(owner.report["deletion_authorized"])
                self.assertIn("closeout refused", owner.report["retention_reason"])
                self.assertNotIn("remove-container", calls)
                self.assertNotIn("remove-volume", calls)

    def test_closed_issue_can_release_retained_volume_without_rewriting_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            operation, calls, present = self.resources(owner)
            owner.root = Path(directory)/"private"
            owner.root.mkdir(mode=0o700)
            owner.root_identity = (owner.root.stat().st_dev, owner.root.stat().st_ino)
            owner.report.update(private_root=str(owner.root), private_root_identity=list(owner.root_identity),
                                primary_error="original business defect", volume_retained=True)
            path, receipt = self.receipt(owner, directory)
            owner.save()
            receipt["revalidation_passed"] = False
            path.write_text(json.dumps(receipt))
            with patch.object(module.Owner, "op") as operation_not_allowed:
                self.assertEqual(module.Owner.close_retained(owner.args.report, path), 1)
                operation_not_allowed.assert_not_called()
            self.assertTrue(owner.root.exists())
            receipt["revalidation_passed"] = True
            path.write_text(json.dumps(receipt))
            with patch.object(module.Owner, "op", side_effect=operation):
                self.assertEqual(module.Owner.close_retained(owner.args.report, path), 0)
            result = json.loads(owner.args.report.read_text())
            self.assertEqual(present, set())
            self.assertLess(calls.index("stop-owned"), calls.index("remove-container"))
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["primary_error"], "original business defect")
            self.assertEqual(result["child_exit"], 101)
            self.assertTrue(result["volume_removed"])
            self.assertFalse(result["volume_retained"])
            self.assertFalse(owner.root.exists())

    def test_explicit_complete_business_acceptance_allows_delete(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            operation, calls, present = self.resources(owner)
            path, receipt = self.receipt(owner, directory)
            receipt.update(kind="business_acceptance", business_success=True, acceptance_complete=True)
            path.write_text(json.dumps(receipt))
            owner.report["child_exit"] = 0
            with patch.object(owner, "op", side_effect=operation):
                owner.cleanup()
            self.assertEqual(present, set())
            self.assertTrue(owner.report["deletion_authorized"])

    def test_retained_bounded_evidence_is_encrypted_and_recoverable(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            owner.root = Path(directory)/"private"
            owner.root.mkdir(mode=0o700)
            owner.private_output = {"stderr": "original private failure bytes"}
            owner.preserve_evidence()
            evidence = owner.report["encrypted_evidence"]
            cipher = Path(evidence["path"])
            self.assertEqual(hashlib.sha256(cipher.read_bytes()).hexdigest(), evidence["sha256"])
            self.assertNotIn(b"original private failure bytes", cipher.read_bytes())
            self.assertEqual(Path(evidence["key_reference"]).stat().st_mode & 0o777, 0o600)
            self.assertEqual(cipher.stat().st_mode & 0o777, 0o600)
            decoded = subprocess.check_output(["openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2", "-in", str(cipher), "-pass", "file:" + evidence["key_reference"]])
            self.assertEqual(json.loads(decoded)["output_tail"]["stderr"], "original private failure bytes")
            self.assertFalse((owner.root/"bounded-evidence.json").exists())
            # Closing the failed database must not destroy the sole key for
            # its retained forensic artifact or rewrite the original failure.
            owner.root_identity = (owner.root.stat().st_dev, owner.root.stat().st_ino)
            operation, calls, present = self.resources(owner)
            owner.report.update(private_root=str(owner.root), private_root_identity=list(owner.root_identity))
            path, receipt = self.receipt(owner, directory)
            owner.save()
            with patch.object(module.Owner, "op", side_effect=operation):
                self.assertEqual(module.Owner.close_retained(owner.args.report, path), 0)
            result = json.loads(owner.args.report.read_text())
            self.assertEqual(present, set())
            self.assertEqual(result["status"], "failed")
            self.assertTrue(Path(result["encrypted_evidence"]["key_reference"]).is_file())
            self.assertTrue(result["private_root_retained"])

    def test_inherited_database_rejected_before_any_docker_or_sql(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            with patch.dict(os.environ, {"COWD_TEST_POSTGRES_URL": "postgresql://foreign/example"}), patch.object(owner, "op") as op:
                with self.assertRaisesRegex(RuntimeError, "refusing inherited"):
                    owner.start()
                op.assert_not_called()

    def test_original_sql_consumer_gets_owned_libpq_fields_without_inheritance(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            owner.root = Path(directory)
            captured = {}
            def launch(argv, **kwargs):
                captured.update(kwargs["env"])
                self.assertEqual(argv[:2], ["bash", str(module.ROOT/"scripts/test/pg-test-env.sh")])
                raise RuntimeError("captured before child launch")
            with patch.dict(os.environ, {"PGHOST": "foreign", "PGPORT": "32769", "PGUSER": "foreign", "PGDATABASE": "foreign", "PGPASSWORD": "foreign", "PGSERVICE": "foreign", "PGOPTIONS": "foreign"}), patch.object(module.subprocess, "Popen", side_effect=launch):
                with self.assertRaisesRegex(RuntimeError, "captured before child launch"):
                    owner.run_child()
            # Literal contract of the original growth_test_sql caller, not a
            # second URL parser or expectations generated by this owner.
            self.assertEqual(captured.get("PGHOST"), "127.0.0.1")
            self.assertEqual(captured.get("PGPORT"), "36435")
            self.assertEqual(captured.get("PGUSER"), "owned_test")
            self.assertEqual(captured.get("PGDATABASE"), "owned_test")
            self.assertEqual(captured.get("PGPASSWORD"), owner.password)
            self.assertIn(":" + owner.password + "@127.0.0.1:36435/owned_test", captured["COWD_TEST_POSTGRES_URL"])
            self.assertNotIn("PGSERVICE", captured)
            self.assertNotIn("PGOPTIONS", captured)

    def test_child_failure_keeps_first_error_even_if_cleanup_also_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            with patch.object(owner, "start"), patch.object(owner, "run_child", side_effect=RuntimeError("original child failure")), patch.object(owner, "cleanup", side_effect=RuntimeError("cleanup failure")) as cleanup:
                self.assertEqual(owner.execute(), 1)
                cleanup.assert_called_once()
            report = json.loads(owner.args.report.read_text())
            self.assertEqual(report["primary_error"], "original child failure")
            self.assertEqual(report["cleanup_errors"], ["cleanup failure"])
            self.assertEqual(report["status"], "failed")

    def test_partial_creation_is_checked_but_foreign_object_never_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            owner.report["container_attempted"] = True
            calls = []
            def operation(name, argv, *args, **kwargs):
                calls.append(name)
                if name == "find-attempted-container":
                    return owner.container
                if name == "inspect-container":
                    return json.dumps([{"Config": {"Labels": {"cowd.test.owner": "foreign"}}}])
                self.fail("unexpected mutation: " + name)
            with patch.object(owner, "op", side_effect=operation):
                owner.cleanup()
            self.assertIn("foreign container ownership", owner.report["cleanup_errors"][0])
            self.assertEqual(calls, ["find-attempted-container", "inspect-container"])

    def test_initial_growth_failure_does_not_prevent_child_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            operation, calls, present = self.resources(owner)
            def child():
                owner.report["child_exit"] = 0
            with patch.object(owner, "start", side_effect=owner.sample_before), patch.object(owner, "sql", side_effect=TimeoutError("growth-before timed out")), patch.object(owner, "run_child", side_effect=child) as run_child, patch.object(owner, "op", side_effect=operation):
                self.assertEqual(owner.execute(), 0)
                run_child.assert_called_once()
            self.assertEqual(owner.report["execution_status"], "passed")
            self.assertEqual(owner.report["diagnostics_status"], "incomplete")
            self.assertNotIn("before", owner.report["samples"])
            self.assertTrue(owner.report["volume_retained"])

    def test_missing_growth_sample_is_separate_from_child_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            def child():
                owner.report["child_exit"] = 0
            def cleanup():
                owner.report["diagnostic_errors"].append("growth sample timed out")
            with patch.object(owner, "start"), patch.object(owner, "run_child", side_effect=child), patch.object(owner, "cleanup", side_effect=cleanup):
                self.assertEqual(owner.execute(), 0)
            report = json.loads(owner.args.report.read_text())
            self.assertEqual(report["child_exit"], 0)
            self.assertEqual(report["status"], "acceptance_pending")
            self.assertEqual(report["execution_status"], "passed")
            self.assertEqual(report["diagnostics_status"], "incomplete")

    def test_low_capacity_is_recorded_without_an_arbitrary_admission_threshold(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = self.owner(directory)
            owner.root = Path(directory)
            from types import SimpleNamespace
            with patch.object(module.shutil, "disk_usage", return_value=SimpleNamespace(free=1)):
                owner.disk_sample(admission=True)
                owner.disk_sample()
            self.assertEqual(len(owner.report["disk_samples"]), 2)
            self.assertTrue(all(value == 1 for value in owner.report["disk_samples"][0]["available_bytes"].values()))


if __name__ == "__main__":
    unittest.main()
