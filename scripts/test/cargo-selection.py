#!/usr/bin/env python3
"""Metadata selection and admission shared by the existing Cargo test lanes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import tomllib


class NotStarted(Exception):
    pass


def run_process(args, cwd, capture_output=True, timeout=120):
    with subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True) as process:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
            raise
        return subprocess.CompletedProcess(args, process.returncode, stdout, stderr)


def command(root, args, timeout=120):
    result = run_process(args, cwd=root, capture_output=True, timeout=timeout)
    if result.returncode:
        raise NotStarted(f"{args[0]} command failed (exit {result.returncode})")
    return result.stdout


def metadata(root):
    return json.loads(command(root, ["cargo", "metadata", "--locked", "--offline",
                                     "--format-version", "1", "--no-deps"]))


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_log(path, data, root=None):
    text = data.decode(errors="replace")
    configured = cargo_environment(root or Path(__file__).resolve().parents[2], check_runner=False)
    for environment in (os.environ, configured):
        for name, value in environment.items():
            if value and re.search(r"TOKEN|PASSWORD|SECRET|API_KEY|CREDENTIAL|PRIVATE_KEY|DATABASE_URL|POSTGRES.*URL", name):
                text = text.replace(value, "<redacted>")
    text = re.sub(r"(?:postgres(?:ql)?|https?)://[^\s\"<>]+", "<redacted-url>", text)
    with path.open("w") as output:
        path.chmod(0o600)
        output.write(text)


def source_identity(root):
    paths = command(root, ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"]).split(b"\0")
    checksum = hashlib.sha256()
    for relative in sorted(set(paths) - {b""}):
        path = root / os.fsdecode(relative)
        checksum.update(relative + b"\0")
        if path.is_symlink():
            checksum.update(os.readlink(path).encode())
        elif path.is_file():
            checksum.update(bytes.fromhex(digest(path.read_bytes())))
        else:
            checksum.update(b"<deleted>")
    for config in config_paths(root):
        if config.is_file():
            checksum.update(str(config.resolve()).encode() + b"\0" + bytes.fromhex(digest(config.read_bytes())))
    return checksum.hexdigest()


def changed_paths(root, base):
    command(root, ["git", "rev-parse", "--verify", base + "^{commit}"])
    paths = command(root, ["git", "diff", "--name-only", "--no-renames", "-z", base, "--"])
    paths += command(root, ["git", "ls-files", "--others", "--exclude-standard", "-z"])
    return sorted(set(os.fsdecode(item) for item in paths.split(b"\0") if item))


def workspace_packages(data):
    return {item["name"]: item for item in data["packages"] if item["id"] in data["workspace_members"]}


def dependency_map(packages):
    directories = {str(Path(item["manifest_path"]).parent.resolve()): name for name, item in packages.items()}
    return {name: {directories[str(Path(dep["path"]).resolve())] for dep in item["dependencies"]
                   if dep.get("path") and str(Path(dep["path"]).resolve()) in directories}
            for name, item in packages.items()}


def closure(selected, edges):
    selected = set(selected)
    while True:
        expanded = selected | {dep for name in selected for dep in edges.get(name, ())}
        if expanded == selected:
            return selected
        selected = expanded


def owners(root, packages, paths):
    directories = sorted(((Path(item["manifest_path"]).parent.relative_to(root).as_posix(), name)
                          for name, item in packages.items()), reverse=True, key=lambda pair: len(pair[0]))
    matched, unmatched = set(), []
    for path in paths:
        owner = next((name for directory, name in directories
                      if directory == "." or path == directory or path.startswith(directory + "/")), None)
        if owner:
            matched.add(owner)
        else:
            unmatched.append(path)
    return matched, unmatched


def make_plan(root, data, paths, quick=False):
    packages = workspace_packages(data)
    edges = dependency_map(packages)
    direct, unmatched = owners(root, packages, paths)
    conservative = bool(unmatched or any(Path(path).name in {"Cargo.toml", "Cargo.lock"}
                                       or path.startswith((".cargo/", "rust-toolchain")) for path in paths))
    if quick or not paths:
        selected = {"harness-contract", "model-protocol", "surface", "matrix-core"}
        if not selected <= packages.keys():
            raise NotStarted("quick boundary packages missing from Cargo metadata")
        reason = "explicit quick boundary feedback; final gates remain required"
    elif conservative:
        selected, reason = set(packages), "manifest/configuration or unmapped path: conservative workspace selection"
    else:
        reverse = {name: {consumer for consumer, deps in edges.items() if name in deps} for name in packages}
        selected, reason = closure(direct, reverse), "direct owners plus transitive normal/dev/build/optional consumers"
    entries = []
    for name in sorted(selected):
        dependencies = closure({name}, edges)
        for target in packages[name]["targets"]:
            kind = next((kind for kind in target["kind"]
                         if kind in {"lib", "rlib", "proc-macro", "bin", "test", "example", "bench"}), None)
            if kind is None:
                continue
            flag = "--lib" if kind in {"lib", "rlib", "proc-macro"} else "--" + kind
            entries.append({"package": name, "package_id": packages[name]["id"], "target": target["name"], "kind": kind,
                            "target_kind": target["kind"],
                            "target_args": [flag] if flag == "--lib" else [flag, target["name"]],
                            "features": "all", "required_features": target.get("required-features", []),
                            "compile_only": not target.get("test", True),
                            "needs_pg": any(dep.endswith("-postgres") for dep in dependencies),
                            "needs_process": bool(dependencies & {"sandbox-launcher", "managed-worker-runtime"}),
                            "model_resource": "offline; live tests remain in canonical ignored/manual lanes",
                            "qualified_selector": None, "expected_selected": None})
    if not entries:
        raise NotStarted("selection has no testable Cargo targets")
    return {"schema": 1, "source": source_identity(root),
            "metadata_sha256": digest(json.dumps(data, sort_keys=True).encode()),
            "reason": reason, "changed_paths": paths, "direct_packages": sorted(direct),
            "unmapped_paths": unmatched, "packages": sorted(selected), "targets": entries,
            "external_checks": [["cargo", "check", "--locked", "--manifest-path", "tests/interactive/Cargo.toml", "--all-targets", "--all-features"]]
            if any(path.startswith("tests/interactive/") for path in paths) else [],
            "status": "Planned", "business_acceptance": "NotAssessed",
            "final_gates": ["scripts/validate.sh full-regression", "scripts/validate.sh contract",
                            "scripts/validate.sh scenario", "scripts/validate.sh surface",
                            "scripts/validate.sh manual postgres-contract", "tests/interactive/Cargo.toml",
                            "canonical ignored/manual/model lanes"]}


def ignored_reasons(root, data):
    markers = re.findall(r"^    marker: (.+)$", (root / "tests/test-governance/test-inventory.yaml").read_text(), re.M)
    reasons = {}
    for name, package in workspace_packages(data).items():
        entries = {}
        for path in Path(package["manifest_path"]).parent.rglob("*.rs"):
            for match in re.finditer(r'#\[ignore(?:\s*=\s*"([^"\n]*)")?\s*\]([\s\S]{0,600}?)\bfn\s+(\w+)\s*\(', path.read_text()):
                reason, _, function = match.groups()
                known = reason and any(marker in reason for marker in markers)
                entries.setdefault(function, set()).add(reason if known else None)
        reasons[name] = entries
    return reasons


def parse_listing(raw):
    names = [line[:-6] for line in raw.decode().splitlines() if line.endswith(": test")]
    if len(names) != len(set(names)):
        raise NotStarted("duplicate discovered test identity")
    return set(names)


def check_source(root, plan):
    if source_identity(root) != plan["source"]:
        raise NotStarted("source changed since plan/discovery")


def config_paths(root):
    directories = [directory / ".cargo" for directory in [root, *root.parents]]
    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
    directories.append(cargo_home)
    return list(dict.fromkeys(directory / ("config" if (directory / "config").is_file() else "config.toml") for directory in directories))


def cargo_environment(root, check_runner=True):
    entries = {}
    for config in reversed(config_paths(root)):
        if not config.is_file():
            continue
        settings = tomllib.loads(config.read_text())
        if check_runner and (settings.get("include") or settings.get("build", {}).get("target") or any("runner" in value for value in settings.get("target", {}).values() if isinstance(value, dict))):
            raise NotStarted("custom target/runner/include requires its original dedicated lane")
        for name, value in settings.get("env", {}).items():
            if isinstance(value, dict):
                entry = {**entries.get(name, {}), **value}
            else:
                entry = {"value": value}
            if entry.get("relative"):
                entry["value"] = str((config.parent.parent / entry["value"]).resolve())
            entries[name] = entry
    if check_runner and (os.environ.get("CARGO_BUILD_TARGET") or any(name.startswith("CARGO_TARGET_") and name.endswith("_RUNNER") for name in os.environ)):
        raise NotStarted("custom target/runner requires its original dedicated lane")
    environment = dict(os.environ)
    for name, entry in entries.items():
        if entry.get("force") or name not in environment:
            environment[name] = entry["value"]
    return environment


def check_resources(root, plan, data=None):
    environment = cargo_environment(root)
    if any(item["needs_pg"] for item in plan["targets"]) and not environment.get("COWD_TEST_POSTGRES_URL"):
        raise NotStarted("missing isolated PG configuration (COWD_TEST_POSTGRES_URL)")
    if any(item["needs_process"] for item in plan["targets"]):
        receipt_path = environment.get("COWD_TEST_LAUNCHER_IDENTITY")
        if not receipt_path:
            raise NotStarted("missing launcher identity receipt (COWD_TEST_LAUNCHER_IDENTITY)")
        receipt = json.loads(Path(receipt_path).read_text())
        launcher = Path(environment.get("COWD_SANDBOX_LAUNCHER_BINARY", ""))
        version = workspace_packages(data).get("sandbox-launcher", {}).get("version") if data else plan.get("launcher_version")
        if (not launcher.is_file() or receipt.get("source") != plan["source"]
                or receipt.get("version") != version or receipt.get("sha256") != digest(launcher.read_bytes())
                or receipt.get("path") != str(launcher.resolve())):
            raise NotStarted("launcher source/version/hash/path mismatch")
        plan["launcher"] = {key: receipt[key] for key in ("source", "version", "sha256", "path")}
        actual = command(root, [str(launcher.resolve()), "__cowd_internal", "sandbox-launcher", "--protocol-version"], timeout=5)
        if actual.decode().strip() != f"sandbox-launcher/{receipt['version']}/kernel-v2":
            raise NotStarted("launcher protocol/version mismatch")


def validate_executables(root, plan):
    check_source(root, plan)
    check_resources(root, plan)
    for path, expected in plan["executables"].items():
        if digest(Path(path).read_bytes()) != expected:
            raise NotStarted("binary changed since discovery")


def native_host(root):
    host = re.search(r"^host: (.+)$", command(root, [os.environ.get("RUSTC", "rustc"), "-vV"]).decode(), re.M)
    if not host:
        raise NotStarted("cannot determine native Cargo runner target")
    return host[1]


def guarded_cargo_args(item, admission, host):
    runner = [sys.executable, str(Path(__file__).resolve()), "launch", str(admission)]
    return ["cargo", "test", "--locked", "-p", item["package"], *item["target_args"], "--all-features",
            "--config", f"target.{host}.runner=" + json.dumps(runner)]


def listed_output(root, args, path):
    result = run_process(args, cwd=root, capture_output=True, timeout=120)
    write_log(path, result.stdout)
    write_log(path.with_suffix(".stderr"), result.stderr)
    if result.returncode:
        for line in result.stdout.splitlines():
            if line.startswith(b'{"status": "NotStarted"'):
                raise NotStarted(json.loads(line)["reason"])
        raise NotStarted(f"Cargo discovery failed (exit {result.returncode}); see listing logs")
    return result.stdout


def discover(root, plan, data, log_dir):
    check_source(root, plan)
    check_resources(root, plan, data)
    for index, args in enumerate(plan["external_checks"]):
        result = run_process(args, cwd=root, capture_output=True, timeout=1200)
        write_log(log_dir / f"external-{index}.stdout", result.stdout)
        write_log(log_dir / f"external-{index}.stderr", result.stderr)
        if result.returncode:
            raise NotStarted(f"external manifest check failed (exit {result.returncode})")
    reasons = ignored_reasons(root, data)
    runners = re.findall(r"^    marker: (.+)\n    runner: (.+)$",
                         (root / "tests/test-governance/test-inventory.yaml").read_text(), re.M)
    plan["launcher_version"] = workspace_packages(data).get("sandbox-launcher", {}).get("version")
    prepared = {}
    host = None
    for index, item in enumerate(plan["targets"]):
        item["build_command"] = ["cargo", "test", "--locked", "-p", item["package"], "--all-targets", "--all-features", "--no-run", "--message-format=json"]
        key = (item["package"], item["features"])
        if key not in prepared:
            result = run_process(item["build_command"], cwd=root, capture_output=True, timeout=1200)
            write_log(log_dir / f"{index}-build.stderr", result.stderr)
            write_log(log_dir / f"{index}-build.jsonl", result.stdout)
            if result.returncode:
                raise NotStarted(f"target build failed (exit {result.returncode}); see build logs")
            artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith(b"{")]
            check_source(root, plan)
            executables = {str(Path(artifact["executable"]).resolve()): digest(Path(artifact["executable"]).read_bytes())
                           for artifact in artifacts if artifact.get("reason") == "compiler-artifact" and artifact.get("executable")}
            prepared[key] = artifacts, executables
        else:
            check_source(root, plan)
        artifacts, executables = prepared[key]
        item["executables"] = executables
        admission = log_dir / f"{index}-launch-admission.json"
        launch_plan = {**plan, "targets": [item], "executables": item["executables"]}
        admission.write_text(json.dumps(launch_plan))
        admission.chmod(0o600)
        candidates = [artifact for artifact in artifacts
                      if artifact.get("reason") == "compiler-artifact" and artifact.get("executable")
                      and artifact.get("package_id") == item["package_id"]
                      and artifact.get("target", {}).get("name") == item["target"]
                      and artifact.get("target", {}).get("kind") == item["target_kind"]
                      and (item["compile_only"] or artifact.get("profile", {}).get("test"))]
        if len(candidates) != 1:
            raise NotStarted("target must produce exactly one libtest executable")
        artifact = candidates[0]
        if not set(item["required_features"]) <= set(artifact.get("features", [])):
            raise NotStarted("built target lacks required features")
        binary = Path(artifact["executable"])
        item.update(binary=str(binary), binary_sha256=executables[str(binary.resolve())])
        if item["compile_only"]:
            item.update(status="CompiledOnly", selectors=[], ignored={}, expected_selected=0)
            continue
        if host is None:
            host = native_host(root)
        item["cargo_args"] = guarded_cargo_args(item, admission, host)
        validate_executables(root, launch_plan)
        listed = listed_output(root, [*item["cargo_args"], "--", "--list", "--format", "terse"], log_dir / f"{index}-list.txt")
        validate_executables(root, launch_plan)
        ignored = listed_output(root, [*item["cargo_args"], "--", "--list", "--ignored", "--format", "terse"], log_dir / f"{index}-ignored.txt")
        names, ignored_names = parse_listing(listed), parse_listing(ignored)
        if not ignored_names <= names:
            raise NotStarted("ignored discovery differs from complete discovery")
        item["ignored"], item["ignored_runners"] = {}, {}
        for name in sorted(ignored_names):
            known = reasons[item["package"]].get(name.split("::")[-1], set())
            if not known or None in known:
                raise NotStarted("unknown ignored reason; register in canonical governance inventory")
            item["ignored"][name] = sorted(known)
            item["ignored_runners"][name] = sorted({runner for reason in known for marker, runner in runners if marker in reason})
            if not item["ignored_runners"][name]:
                raise NotStarted("ignored test lacks canonical runner")
        item["selectors"] = sorted(names - ignored_names)
        item["expected_selected"] = len(item["selectors"])
        item["status"] = "Discovered" if item["selectors"] else "CanonicalManualOnly" if ignored_names else "CompiledOnly"
        check_source(root, plan)
    if not any(item["expected_selected"] for item in plan["targets"]):
        raise NotStarted("zero selected tests; no executable regression coverage")
    plan["status"] = "Discovered"


def execute(root, plan, data, log_dir):
    check_source(root, plan)
    check_resources(root, plan, data)
    for index, item in enumerate(plan["targets"]):
        check_resources(root, plan, data)
        if digest(Path(item["binary"]).read_bytes()) != item["binary_sha256"]:
            raise NotStarted("binary changed since discovery")
        check_source(root, plan)
        if not item["expected_selected"]:
            continue
        item["status"] = "Running"
        result = run_process([*item["cargo_args"], "--", "--format", "pretty"], cwd=root, capture_output=True,
                                timeout=int(os.environ.get("COWD_TEST_TIMEOUT_SECONDS", "1200")))
        write_log(log_dir / f"{index}-run.stdout", result.stdout)
        write_log(log_dir / f"{index}-run.stderr", result.stderr)
        item["command_exit"] = result.returncode
        summaries = re.findall(rb"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", result.stdout)
        item["status"] = "TestsPassed" if result.returncode == 0 else "TestsFailed"
        if len(summaries) != 1:
            raise NotStarted("missing or ambiguous execution summary")
        item["exit"] = result.returncode
        passed, failed, ignored = map(int, summaries[0])
        observed = re.findall(rb"^test (.+?) \.\.\. (ok|FAILED|ignored)(?:,.*)?$", result.stdout, re.M)
        executed = [name.decode() for name, state in observed if state != b"ignored"]
        skipped = [name.decode() for name, state in observed if state == b"ignored"]
        if sorted(executed) != item["selectors"] or sorted(skipped) != sorted(item["ignored"]):
            raise NotStarted("executed selector set differs from discovery")
        if passed + failed != item["expected_selected"] or ignored != len(item["ignored"]):
            raise NotStarted("executed count differs from discovery")
        check_source(root, plan)
        if digest(Path(item["binary"]).read_bytes()) != item["binary_sha256"]:
            raise NotStarted("binary changed during execution")
        if result.returncode:
            plan["status"] = "TestsFailed"
            return result.returncode
    plan["status"] = "TestsPassed"
    return 0


def main():
    if len(sys.argv) >= 5 and sys.argv[1] == "launch":
        root = Path(__file__).resolve().parents[2]
        try:
            admission = json.loads(Path(sys.argv[2]).read_text())
            validate_executables(root, admission)
            binary = str(Path(sys.argv[3]).resolve())
            if binary not in admission["executables"]:
                raise NotStarted("Cargo selected an unbound executable")
            os.execv(binary, [binary, *sys.argv[4:]])
        except (NotStarted, OSError, ValueError, KeyError, TypeError) as error:
            print(json.dumps({"status": "NotStarted", "reason": str(error) if isinstance(error, NotStarted) else type(error).__name__}))
            return 2
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["plan", "discover", "run", "owners", "report"])
    parser.add_argument("--base", default=os.environ.get("COWD_CHANGED_BASE", "HEAD"))
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--workspace", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("paths", nargs="*")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if args.mode == "report":
        directory = Path(os.environ.get("COWD_TEST_EVIDENCE_ROOT", str(root.parent / ".cowd-test-evidence"))).resolve()
        if directory.is_relative_to(root):
            parser.error("evidence root must be outside the source tree")
        print(directory / f"selection-{time.time_ns()}-{os.getpid()}.json")
        return 0
    plan = {"status": "NotStarted", "business_acceptance": "NotAssessed"}
    status = 0
    try:
        if args.output and (args.output.resolve().is_relative_to(root) or args.output.exists()):
            args.output = None
            raise NotStarted("report must be a new file outside the source tree")
        data = metadata(root)
        paths = ["Cargo.toml"] if args.workspace else args.paths or ([] if args.quick else changed_paths(root, args.base))
        if args.mode == "owners":
            matched, _ = owners(root, workspace_packages(data), args.paths)
            print("\n".join(sorted(matched)))
            return 0
        plan = make_plan(root, data, paths, args.quick)
        if args.plan and json.loads(args.plan.read_text()) != plan:
            raise NotStarted("stale or modified plan; regenerate from current metadata/source")
        if args.mode != "plan":
            if not args.output or args.output.resolve().is_relative_to(root):
                raise NotStarted("discovery/execution requires --output outside the source tree")
            args.output.parent.mkdir(parents=True, exist_ok=True)
            log_dir = args.output.with_suffix(".logs")
            log_dir.mkdir(mode=0o700, exist_ok=False)
            discover(root, plan, data, log_dir)
            if args.mode == "run":
                status = execute(root, plan, data, log_dir)
    except (NotStarted, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
        started = any("exit" in item or item.get("status") == "Running" for item in plan.get("targets", []))
        plan["status"] = "Incomplete" if started else "NotStarted"
        plan["reason"] = str(error) if isinstance(error, NotStarted) else type(error).__name__
        if isinstance(error, subprocess.TimeoutExpired) and "log_dir" in locals():
            write_log(log_dir / "timeout.stdout", error.stdout or b"")
            write_log(log_dir / "timeout.stderr", error.stderr or b"")
        status = 2
    encoded = json.dumps(plan, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as output:
            args.output.chmod(0o600)
            output.write(encoded)
    else:
        print(encoded, end="")
    return status


if __name__ == "__main__":
    sys.exit(main())
