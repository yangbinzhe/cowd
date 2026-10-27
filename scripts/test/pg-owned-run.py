#!/usr/bin/env python3
"""One exclusively owned local PG per test command; finally stops its processes.

This is the outer owner of pg-test-env.sh, not another test scheduler. Never
accept a supplied database URL. Failed or unaccepted databases remain until a
resource-bound acceptance/issue-closure receipt authorizes deletion. SIGKILL or
host loss cannot execute finally; the manifest identifies unfinished cleanup.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import selectors
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT.parent / ".cowd-build/next-kernel-dev-20261022"
IMAGE = "sha256:de3a4eab8fdfa507ea92aac488b916b08089e515db49b055fe71dfa271ba3a28"
SAMPLE = """
WITH namespaces AS (
 SELECT oid,nspname FROM pg_namespace
 WHERE nspname !~ '^pg_' AND nspname <> 'information_schema'
), tables AS (
 SELECT c.oid,n.nspname,c.relname,c.reltuples,c.reltoastrelid
 FROM pg_class c JOIN namespaces n ON n.oid=c.relnamespace
 WHERE c.relkind IN ('r','p','m')
), sizes AS (
 SELECT *,pg_total_relation_size(oid) AS total_bytes,
 pg_table_size(oid)-CASE WHEN reltoastrelid=0 THEN 0 ELSE pg_total_relation_size(reltoastrelid) END AS heap_bytes,
 pg_indexes_size(oid) AS index_bytes,
 CASE WHEN reltoastrelid=0 THEN 0 ELSE pg_total_relation_size(reltoastrelid) END AS toast_bytes
 FROM tables
)
SELECT jsonb_build_object(
 'database_bytes',pg_database_size(current_database()),
 'namespace_count',(SELECT count(*) FROM namespaces),
 'empty_namespace_count',(SELECT count(*) FROM namespaces n WHERE NOT EXISTS
   (SELECT 1 FROM pg_class c WHERE c.relnamespace=n.oid)),
 'table_count',(SELECT count(*) FROM tables),
 'relation_count',(SELECT count(*) FROM pg_class c JOIN namespaces n ON n.oid=c.relnamespace),
 'top20',COALESCE((SELECT jsonb_agg(to_jsonb(t)) FROM (
   SELECT nspname AS schema,relname AS table_name,heap_bytes,index_bytes,toast_bytes,total_bytes,
   CASE WHEN reltuples>=0 THEN reltuples::bigint ELSE NULL END AS row_estimate
   FROM sizes ORDER BY total_bytes DESC,nspname,relname LIMIT 20)t),'[]'::jsonb))::text;
"""


def process(pid):
    fields = (Path("/proc") / str(pid) / "stat").read_text().rsplit(") ", 1)[1].split()
    return {"pid": pid, "ppid": int(fields[1]), "pgid": int(fields[2]), "starttime": fields[19]}


class Owner:
    def __init__(self, args):
        self.args, self.root, self.child = args, None, None
        self.nonce = secrets.token_hex(8)
        self.container = "cowd-test-" + args.run_id[:24] + "-" + self.nonce
        self.volume = self.container + "-data"
        self.labels = {"cowd.test.run": args.run_id, "cowd.test.owner": self.nonce}
        self.password = secrets.token_hex(32)
        self.owned = {}
        self.interrupted = 0
        self.private_output = {}
        self.deadline = time.monotonic() + 45
        self.report = {"run_id": args.run_id, "owner": os.getpid(), "status": "NotStarted",
                       "container": self.container, "volume": self.volume, "port": args.port,
                       "samples": {}, "disk_samples": [], "cleanup_errors": [], "diagnostic_errors": [],
                       "operations": [], "no_supplied_database": True}

    def deletion_authorized(self):
        """Only the external owner supplies sign-off, never child exit/output."""
        receipt_path = getattr(self.args, "closeout_receipt", None)
        self.report["deletion_authorized"] = False
        if receipt_path is None:
            self.report["retention_reason"] = "business acceptance unknown; no closeout receipt"
            return False
        try:
            receipt_path = receipt_path.resolve(strict=True)
            if self.root and receipt_path.is_relative_to(self.root.resolve()):
                raise ValueError("closeout receipt cannot come from the child private root")
            metadata = receipt_path.stat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_size > 65536:
                raise ValueError("closeout receipt must be a bounded owner-only 0600 file")
            raw = receipt_path.read_bytes()
            receipt = json.loads(raw)
            if not isinstance(receipt, dict):
                raise ValueError("closeout receipt must be an object")
            expected = {"run_id": self.args.run_id, "container": self.container,
                        "volume": self.volume, "owner_nonce": self.nonce}
            if any(receipt.get(key) != value for key, value in expected.items()):
                raise ValueError("foreign closeout receipt resource identity")
            kind = receipt.get("kind")
            if kind == "business_acceptance":
                if receipt.get("business_success") is not True or receipt.get("acceptance_complete") is not True or self.report.get("child_exit") != 0 or self.report.get("primary_error") or self.interrupted or self.report.get("interrupted_signal") or self.report["cleanup_errors"] or self.report["diagnostic_errors"]:
                    raise ValueError("business acceptance incomplete or execution failed")
            elif kind == "issue_closure":
                if any(not isinstance(receipt.get(key), str) or not receipt[key].strip()
                       for key in ("issue", "root_cause", "fix")) or receipt.get("revalidation_passed") is not True:
                    raise ValueError("named issue has no root cause, fix and successful revalidation")
            else:
                raise ValueError("unknown closeout receipt kind")
            # The owner references the actual independent acceptance/revalidation
            # artifact; a boolean or the child's exit status alone is insufficient.
            evidence = receipt["evidence"]
            if not isinstance(evidence, dict):
                raise ValueError("closeout evidence must be an object")
            path = Path(evidence["path"])
            if not path.is_absolute() or (self.root and path.resolve().is_relative_to(self.root.resolve())):
                raise ValueError("closeout evidence must be outside the child private root")
            with path.open("rb") as handle:
                digest = hashlib.file_digest(handle, "sha256").hexdigest()
            if digest != evidence["sha256"]:
                raise ValueError("closeout evidence digest mismatch")
            self.report["closeout"] = {"path": str(receipt_path), "sha256": hashlib.sha256(raw).hexdigest(),
                                       "kind": kind, "evidence": evidence}
            self.report["deletion_authorized"] = True
            self.report.pop("retention_reason", None)
            return True
        except (OSError, ValueError, TypeError, KeyError) as error:
            self.report["retention_reason"] = self.redact("closeout refused: " + str(error))
            return False

    def preserve_evidence(self):
        if not self.root or self.report.get("encrypted_evidence"):
            return
        # Keep the password/config directory for recovery of this exact volume.
        # Encrypt bounded output tails and the manifest, not a claimed full dump.
        plain, key = self.root / "bounded-evidence.json", self.root / "evidence.key"
        cipher = self.args.report.with_name(self.args.report.name + ".evidence.enc")
        if cipher.exists():
            raise RuntimeError("refusing to overwrite encrypted evidence")
        for path, contents in ((plain, json.dumps({"report": self.report, "output_tail": self.private_output})),
                               (key, secrets.token_hex(32))):
            with open(path, "x", opener=lambda p, flags: os.open(p, flags, 0o600)) as handle:
                handle.write(contents)
        self.args.report.parent.mkdir(parents=True, exist_ok=True)
        # Create the destination privately before openssl opens it.
        with open(cipher, "x", opener=lambda p, flags: os.open(p, flags, 0o600)):
            pass
        self.op("encrypt-retained-evidence", ["openssl", "enc", "-aes-256-cbc", "-pbkdf2",
                "-salt", "-in", str(plain), "-out", str(cipher), "-pass", "file:" + str(key)], seconds=5)
        with cipher.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        self.report["encrypted_evidence"] = {"path": str(cipher), "sha256": digest,
            "key_reference": str(key), "scope": "manifest and last 64KiB of each child stream; not full wire or PG dump"}
        plain.unlink()

    def save(self):
        self.args.report.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.args.report.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.report, indent=2) + "\n")
        temporary.chmod(0o600)
        temporary.replace(self.args.report)

    def op(self, name, argv, env=None, seconds=10):
        remaining = min(seconds, self.deadline - time.monotonic())
        if remaining <= 0:
            raise TimeoutError("owned phase deadline: " + name)
        p = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             start_new_session=True)
        row = {"operation": name, **process(p.pid)}
        self.report["operations"].append(row)
        try:
            out, err = p.communicate(timeout=remaining)
        except BaseException:
            os.killpg(p.pid, signal.SIGKILL)
            p.communicate(timeout=3)
            raise
        finally:
            row["exit"] = p.returncode
            row["reaped"] = p.returncode is not None
        if p.returncode:
            raise RuntimeError(name + ": " + self.redact(err.decode(errors="replace")[:2048]))
        if len(out) > 256 * 1024:
            raise RuntimeError(name + ": bounded operation output exceeded")
        return out.decode().strip()

    def redact(self, text):
        text = text.replace(self.password, "[private]")
        return re.sub(r"postgres(?:ql)?://[^\s\"']+", "[owned-pg-url]", text)

    def inspect(self, resource, name):
        raw = self.op("inspect-" + resource, ["docker", resource, "inspect", name])
        item = json.loads(raw)[0]
        labels = item["Config"]["Labels"] if resource == "container" else item["Labels"]
        if not labels or any(labels.get(k) != v for k, v in self.labels.items()):
            raise RuntimeError("foreign " + resource + " ownership; refusing mutation")
        return item

    def sql(self, name, sql):
        env = {k: os.environ[k] for k in ("PATH", "LANG") if k in os.environ}
        env.update(PGHOST="127.0.0.1", PGPORT=str(self.args.port), PGUSER="owned_test",
                   PGDATABASE="owned_test", PGPASSWORD=self.password, PGCONNECT_TIMEOUT="2",
                   PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=2000 -c lock_timeout=200")
        return self.op(name, ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", sql], env, 5)

    def disk_sample(self, admission=False):
        free = {str(p): shutil.disk_usage(p).free for p in (ROOT.parent, Path("/"), self.root)}
        self.report["disk_samples"].append({"at": time.time(), "available_bytes": free})
        if free[str(ROOT.parent)] < (20 if admission else 10) * 1024**3:
            raise RuntimeError("owned Datas capacity admission failed")
        if free["/"] < (10 if admission else 8) * 1024**3 or free[str(self.root)] < 2 * 1024**3:
            raise RuntimeError("owned root/private capacity admission failed")

    def start(self):
        if os.environ.get("COWD_TEST_POSTGRES_URL") or os.environ.get("COWD_TEST_POSTGRES_TARGET_URL"):
            raise RuntimeError("refusing inherited PostgreSQL URL; this runner creates its own database")
        endpoint = self.op("local-docker", ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"])
        if not endpoint.startswith("unix://"):
            raise RuntimeError("only local Docker is admitted")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", self.args.port))
        if self.op("local-image", ["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"] ) != IMAGE:
            raise RuntimeError("original local PG image missing; no pull permitted")
        self.root = Path(tempfile.mkdtemp(prefix="cowd-pg-owned-"))
        self.root.chmod(0o700)
        self.root_identity = (self.root.stat().st_dev, self.root.stat().st_ino)
        for name in ("workspace", "tmp", "home"):
            (self.root / name).mkdir(mode=0o700)
        secret = self.root / "postgres-password"
        secret.write_text(self.password + "\n")
        secret.chmod(0o600)
        self.report.update(private_root=str(self.root), status="starting", labels=self.labels,
                           private_root_identity=list(self.root_identity), image=IMAGE,
                           deadline_seconds=self.args.seconds, evidence_seconds=15)
        self.disk_sample(True)
        self.save()
        labels = [arg for pair in self.labels.items() for arg in ("--label", "=".join(pair))]
        self.report["volume_attempted"] = True
        self.save()
        self.op("create-volume", ["docker", "volume", "create", *labels, self.volume])
        self.report["volume_created"] = True
        self.report["container_attempted"] = True
        self.save()
        self.op("create-container", ["docker", "run", "--detach", "--pull", "never", "--name", self.container,
                *labels, "--env", "POSTGRES_USER=owned_test", "--env", "POSTGRES_DB=owned_test",
                "--env", "POSTGRES_PASSWORD_FILE=/run/secrets/owned-password",
                "--mount", f"type=bind,source={secret},target=/run/secrets/owned-password,readonly",
                "--mount", f"type=volume,source={self.volume},target=/var/lib/postgresql/data",
                "--log-opt", "max-size=2m", "--log-opt", "max-file=1", "--publish",
                f"127.0.0.1:{self.args.port}:5432", IMAGE, "postgres", "-c", "max_connections=128"], seconds=20)
        self.report["container_created"] = True
        item = self.inspect("container", self.container)
        if item["Image"] != IMAGE or item["NetworkSettings"]["Ports"]["5432/tcp"] != [{"HostIp": "127.0.0.1", "HostPort": str(self.args.port)}]:
            raise RuntimeError("owned PG image/port identity mismatch")
        self.report["PG_process"] = process(item["State"]["Pid"])
        while True:
            try:
                identity = self.sql("host-tcp-readiness", "SELECT current_database()||'|'||current_user||'|'||inet_server_port()::text")
            except RuntimeError:
                if time.monotonic() >= self.deadline or self.interrupted:
                    raise
                time.sleep(.2)
                continue
            if identity != "owned_test|owned_test|5432":
                raise RuntimeError("foreign host TCP identity")
            break
        self.report["samples"]["before"] = json.loads(self.sql("growth-before", SAMPLE))

    def track(self):
        rows = {}
        for d in Path("/proc").iterdir():
            if d.name.isdecimal():
                try:
                    rows[int(d.name)] = process(int(d.name))
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass
        changed = True
        while changed:
            changed = False
            for pid, row in rows.items():
                if pid not in self.owned and (row["ppid"] == os.getpid() or
                        row["ppid"] in self.owned or (self.child and row["pgid"] == self.child.pid)):
                    self.owned[pid] = row
                    changed = True
        return [row for pid, row in rows.items() if pid in self.owned and
                row["starttime"] == self.owned[pid]["starttime"]]

    def run_child(self):
        # Same original availability validator; it no longer owns an unbounded
        # caller-supplied database in this execution path.
        env = {k: os.environ[k] for k in ("PATH", "LANG", "LD_LIBRARY_PATH") if k in os.environ}
        env.update(HOME=str(self.root / "home"), TMPDIR=str(self.root / "tmp"),
                   CARGO_HOME=str(Path.home() / ".cargo"), RUSTUP_HOME=str(Path.home() / ".rustup"),
                   CARGO_TARGET_DIR=str(TARGET), CARGO_NET_OFFLINE="true", CARGO_BUILD_JOBS="4",
                   COWD_TEST_POSTGRES_URL=f"postgresql://owned_test:{self.password}@127.0.0.1:{self.args.port}/owned_test",
                   COWD_R6_OWNED_ROOT=str(self.root / "workspace"))
        if os.environ.get("COWD_BIN"):
            env["COWD_BIN"] = os.environ["COWD_BIN"]
        executable = shutil.which(self.args.command[0])
        if executable:
            digest = hashlib.sha256()
            with open(executable, "rb") as source:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(block)
            self.report["child_executable"] = {"path": str(Path(executable).resolve()), "sha256": digest.hexdigest()}
        self.child = subprocess.Popen(["bash", str(ROOT / "scripts/test/pg-test-env.sh"), *self.args.command],
                    cwd=self.root / "workspace", env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    start_new_session=True)
        self.owned[self.child.pid] = process(self.child.pid)
        self.report.update(status="running", child_identity=self.owned[self.child.pid])
        self.save()
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        counts = {"stdout": 0, "stderr": 0}
        mux = selectors.DefaultSelector()
        for pipe, name in ((self.child.stdout, "stdout"), (self.child.stderr, "stderr")):
            os.set_blocking(pipe.fileno(), False)
            mux.register(pipe, selectors.EVENT_READ, name)
        end, next_disk = time.monotonic() + self.args.seconds, time.monotonic() + 15
        try:
            while mux.get_map() or self.child.poll() is None:
                self.track()
                if self.interrupted or time.monotonic() >= end:
                    raise TimeoutError("owned child interrupted/deadline")
                if time.monotonic() >= next_disk:
                    self.disk_sample()
                    next_disk = time.monotonic() + 15
                for key, _ in mux.select(.1):
                    chunk = os.read(key.fileobj.fileno(), 8192)
                    if not chunk:
                        mux.unregister(key.fileobj)
                    else:
                        counts[key.data] += len(chunk)
                        buffers[key.data].extend(chunk)
                        del buffers[key.data][:-65536]
            self.child.wait(timeout=1)
        finally:
            mux.close()
            self.private_output = {k: v.decode(errors="replace") for k, v in buffers.items()}
            self.report["child_output_bytes"] = counts
            self.report["child_output_tail"] = {k: self.redact(v.decode(errors="replace")[-4096:]) for k, v in buffers.items()}
            self.report["child_exit"] = self.child.poll()

    def cleanup(self):
        self.deadline = time.monotonic() + 45
        # Reap the exact child/descendants even if the leader already exited.
        for sig, seconds in ((signal.SIGTERM, 3), (signal.SIGKILL, 3)):
            live = self.track() if self.child else []
            for row in live:
                try:
                    if process(row["pid"])["starttime"] == row["starttime"]:
                        os.kill(row["pid"], sig)
                except (ProcessLookupError, FileNotFoundError):
                    pass
            until = time.monotonic() + seconds
            while live and time.monotonic() < until:
                if self.child and self.child.poll() is not None:
                    self.child.wait()
                try:
                    while os.waitpid(-1, os.WNOHANG)[0] > 0:
                        pass
                except ChildProcessError:
                    pass
                live = self.track()
                if live:
                    time.sleep(.05)
        self.report["owned_processes"] = list(self.owned.values())
        self.report["remaining_processes"] = self.track() if self.child else []
        if self.child:
            self.report["child_exit"] = self.child.poll()
            self.report["child_reaped"] = self.child.returncode is not None
            self.child.stdout.close()
            self.child.stderr.close()
        if self.report["remaining_processes"]:
            self.report["cleanup_errors"].append("owned child process remains")
        # Diagnostics have their own finite budget; failure retains the database.
        if self.report.get("container_created"):
            try:
                self.deadline = time.monotonic() + 15
                self.inspect("container", self.container)
                self.report["samples"]["after"] = json.loads(self.sql("growth-after", SAMPLE))
                self.report["remaining_db_clients"] = int(self.sql("clients-after", "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid()"))
                if self.report["remaining_db_clients"]:
                    self.report["cleanup_errors"].append("original clients still present before PG removal")
                before, after = self.report["samples"].get("before"), self.report["samples"]["after"]
                if before:
                    self.report["growth_delta"] = {k: after[k]-before[k] for k in ("database_bytes", "namespace_count", "empty_namespace_count", "table_count", "relation_count")}
                largest = after["top20"][:1]
                if largest and largest[0]["table_name"] in ("runtime_events", "session_events"):
                    schema, table = largest[0]["schema"], largest[0]["table_name"]
                    if not re.fullmatch(r"[a-zA-Z0-9_]+", schema):
                        raise RuntimeError("unsupported event namespace for optional diagnostic")
                    columns = self.sql("event-columns", f"SELECT attname FROM pg_attribute WHERE attrelid='\"{schema}\".\"{table}\"'::regclass AND attnum>0 AND NOT attisdropped").splitlines()
                    event_column = "kind" if "kind" in columns else "event_type" if "event_type" in columns else None
                    for label, column in (("event_types", event_column), ("writer_callers", "caller" if "caller" in columns else None)):
                        if column:
                            self.report[label] = json.loads(self.sql(label, f"SELECT COALESCE(json_agg(t),'[]') FROM (SELECT \"{column}\" AS value,count(*) AS count FROM \"{schema}\".\"{table}\" GROUP BY 1 ORDER BY 2 DESC LIMIT 20)t"))
                        else:
                            self.report[label] = {"status": "unknown", "reason": "no durable column; not inferred from event kinds"}
            except Exception as error:
                self.report["diagnostic_errors"].append(self.redact(str(error)))
        self.release_resources()

    def release_resources(self):
        # Stopping processes and deleting evidence are separate decisions.
        initial_errors = len(self.report["cleanup_errors"])
        remove = self.deletion_authorized() and not self.report.get("remaining_processes")
        self.deadline = time.monotonic() + 30
        for resource, attempted, name in (("container", "container_attempted", self.container), ("volume", "volume_attempted", self.volume)):
            if not self.report.get(attempted):
                continue
            try:
                # Creation can succeed before its client times out. Check the
                # exact attempted name, not just a returned-success flag.
                names = self.op("find-attempted-"+resource,
                    ["docker", resource, "ls", *(["-a"] if resource == "container" else []),
                     "--filter", "name=^"+("/" if resource == "container" else "")+name+"$",
                     "--format", "{{.Names}}" if resource == "container" else "{{.Name}}"])
                if name not in names.splitlines():
                    self.report[resource+"_removed"] = True
                    self.report[resource+"_retained"] = False
                    continue
                self.inspect(resource, name)
                if resource == "container":
                    self.op("stop-owned", ["docker", "stop", "--time", "5", name], seconds=12)
                    stopped = self.inspect(resource, name)
                    if stopped["State"]["Running"]:
                        raise RuntimeError("owned PG still running after stop")
                    self.report["container_stopped"] = True
                if not remove or self.interrupted or len(self.report["cleanup_errors"]) != initial_errors:
                    self.report[resource+"_retained"] = True
                    continue
                self.op("remove-"+resource, ["docker", resource, "rm", name])
                names = self.op("verify-absent-"+resource, ["docker", resource, "ls", "-aq" if resource == "container" else "--quiet", "--filter", "label=cowd.test.owner="+self.nonce])
                if names:
                    raise RuntimeError(resource + " was not removed")
                self.report[resource+"_removed"] = True
                self.report[resource+"_retained"] = False
            except Exception as error:
                self.report["cleanup_errors"].append(self.redact(str(error)))
        pg_process = self.report.get("PG_process")
        if pg_process:
            try:
                live = process(pg_process["pid"])["starttime"] == pg_process["starttime"]
            except (FileNotFoundError, ProcessLookupError):
                live = False
            self.report["PG_process_returned"] = not live
            if live:
                self.report["cleanup_errors"].append("owned PG host process still present")
        if self.root:
            try:
                st = self.root.lstat()
                if (st.st_dev, st.st_ino) != self.root_identity or not stat.S_ISDIR(st.st_mode) or stat.S_IMODE(st.st_mode) != 0o700 or st.st_uid != os.getuid():
                    raise RuntimeError("private root identity changed; refusing deletion")
                if remove and not self.interrupted and len(self.report["cleanup_errors"]) == initial_errors:
                    if self.report.get("encrypted_evidence"):
                        # Do not destroy the only key while retaining ciphertext.
                        self.report["private_root_retained"] = True
                        self.report["private_root_retention_reason"] = "encrypted evidence key/config custody remains with original owner"
                    else:
                        shutil.rmtree(self.root)
                        self.report["private_root_removed"] = not self.root.exists()
                        self.report["private_root_retained"] = False
                else:
                    self.report["private_root_retained"] = True
                    self.deadline = time.monotonic() + 5
                    self.preserve_evidence()
            except Exception as error:
                self.report["cleanup_errors"].append(self.redact(str(error)))
        try:
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", self.args.port))
            self.report["port_bindable"] = True
        except Exception as error:
            self.report["cleanup_errors"].append(self.redact(str(error)))

    @classmethod
    def close_retained(cls, report_path, receipt_path):
        """Reuse this owner/finalizer after the named issue has been resolved."""
        report = json.loads(report_path.read_text())
        args = argparse.Namespace(run_id=report["run_id"], port=report["port"], seconds=1,
                                  report=report_path, command=[], closeout_receipt=receipt_path)
        owner = cls(args)
        owner.report = report
        owner.nonce = report["labels"]["cowd.test.owner"]
        owner.labels = {"cowd.test.run": args.run_id, "cowd.test.owner": owner.nonce}
        owner.container = "cowd-test-" + args.run_id[:24] + "-" + owner.nonce
        owner.volume = owner.container + "-data"
        if report["container"] != owner.container or report["volume"] != owner.volume or report["labels"] != owner.labels:
            raise ValueError("retained manifest resource identity mismatch")
        owner.root = Path(report["private_root"])
        owner.root_identity = tuple(report["private_root_identity"])
        for row in report.get("owned_processes", []):
            try:
                if process(row["pid"])["starttime"] == row["starttime"]:
                    raise RuntimeError("original owned process still live; refusing retained closure")
            except (FileNotFoundError, ProcessLookupError):
                pass
        # Re-evaluate process return; preserve the original cleanup diagnostics.
        owner.report["remaining_processes"] = []
        # Closing a past failure never rewrites its original business result.
        if not owner.deletion_authorized():
            owner.save()
            return 1
        errors_before = len(owner.report["cleanup_errors"])
        owner.release_resources()
        owner.report["closure_finished_at"] = time.time()
        owner.save()
        return 0 if len(owner.report["cleanup_errors"]) == errors_before and owner.report.get("volume_removed") and owner.report.get("container_removed") and owner.report.get("port_bindable") and owner.report.get("PG_process_returned", True) else 1

    def execute(self):
        previous = {}
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            previous[sig] = signal.signal(sig, lambda number, frame: setattr(self, "interrupted", number))
        try:
            if ctypes.CDLL(None).prctl(36, 1, 0, 0, 0) != 0:
                raise RuntimeError("Linux subreaper admission failed")
            self.start()
            self.run_child()
        except Exception as error:
            self.report["primary_error"] = self.redact(str(error))
        finally:
            try:
                self.cleanup()
            except Exception as error:
                self.report["cleanup_errors"].append(self.redact(str(error)))
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            execution_ok = self.report.get("child_exit") == 0 and not self.report["cleanup_errors"] and not self.report["diagnostic_errors"] and not self.report.get("primary_error") and not self.interrupted
            self.report["status"] = ("passed" if self.report.get("deletion_authorized") else "acceptance_pending") if execution_ok else "failed"
            self.report["interrupted_signal"] = self.interrupted
            self.report["finished_at"] = time.time()
            self.save()
        if self.interrupted:
            return 128 + self.interrupted
        return self.report.get("child_exit") or (1 if self.report["status"] != "passed" else 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id")
    parser.add_argument("--port", type=int)
    parser.add_argument("--seconds", type=int)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--closeout-receipt", type=Path)
    parser.add_argument("--close-retained", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command.pop(0)
    if args.close_retained:
        if not args.report.is_absolute() or not args.report.is_file() or not args.closeout_receipt or args.command or args.run_id or args.port or args.seconds:
            parser.error("close-retained requires only original report and closeout receipt")
        return Owner.close_retained(args.report, args.closeout_receipt)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", args.run_id or "") or args.port is None or not 1024 < args.port < 65536 or args.port == 32769 or args.seconds is None or not 1 <= args.seconds <= 1800 or not args.command or not args.report.is_absolute() or args.report.exists():
        parser.error("new safe run-id/port/report, finite seconds and explicit child command required")
    return Owner(args).execute()


if __name__ == "__main__":
    sys.exit(main())
