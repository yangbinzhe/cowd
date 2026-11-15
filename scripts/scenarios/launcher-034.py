#!/usr/bin/env python3
"""Reuse the 032 single-slot preparation and controller after identity binding."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

HERE = Path(__file__).resolve().parent
BUILD = Path('/media/yi/Datas/workspace/.cowd-build')
REPO = Path('/media/yi/Datas/workspace/cowd')
EVIDENCE = REPO.parent / 'plan/2026-10/1022-cowd-next-kernel-unification/evidence/n7ab-parallel'
TREES = REPO.parent / '.cowd-worktrees'

def run(*args):
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout.strip()

def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for data in iter(lambda: f.read(1024 * 1024), b''):
            h.update(data)
    return h.hexdigest()

def identity():
    try:
        data = json.loads((HERE / 'identity.json').read_text())
    except (OSError, ValueError) as error:
        print(f'warning: candidate identity unavailable: {error}', file=sys.stderr)
        data = {}
    required = ('version', 'commit', 'tree', 'source_sha256', 'host_worktree',
                'cowd_path', 'cowd_sha256', 'harness_path', 'harness_sha256',
                'lib_test_path', 'lib_test_sha256')
    missing = [key for key in required if not isinstance(data.get(key), str) or not data[key]]
    if missing:
        print('warning: candidate identity incomplete: ' + ', '.join(missing), file=sys.stderr)
    host = Path(data.get('host_worktree') or HERE.parents[1])
    head = run('git', '-C', str(host), 'rev-parse', 'HEAD')
    tree = run('git', '-C', str(host), 'rev-parse', 'HEAD^{tree}')
    diff = subprocess.run(['git', '-C', str(host), 'diff', '--binary', 'HEAD'], check=True, capture_output=True).stdout
    data.update(commit=head, tree=tree, host_worktree=str(host), diff_sha256=hashlib.sha256(diff).hexdigest())
    data['version'] = data.get('version') or 'unversioned'
    target = Path(os.environ.get('CARGO_TARGET_DIR', str(BUILD / 'default-target'))) / 'debug'
    for path_key, sha_key, name in [('cowd_path', 'cowd_sha256', 'cowd'),
                                    ('harness_path', 'harness_sha256', 'harness-eval'),
                                    ('lib_test_path', 'lib_test_sha256', 'lib-test')]:
        data[path_key] = data.get(path_key) or str(target / name)
        if Path(data[path_key]).is_file():
            data[sha_key] = digest(data[path_key])
        else:
            print(f'warning: candidate binary unavailable: {data[path_key]}', file=sys.stderr)
            data[sha_key] = ''
    proc = subprocess.Popen(['git', '-C', str(host), 'archive', head, 'crates'], stdout=subprocess.PIPE)
    h = hashlib.sha256()
    for chunk in iter(lambda: proc.stdout.read(1024 * 1024), b''):
        h.update(chunk)
    proc.stdout.close()
    if proc.wait():
        print('warning: candidate source archive unavailable', file=sys.stderr)
    data['source_sha256'] = h.hexdigest()
    return data

def slot(slug, route):
    entries = json.loads((HERE / 'slots.json').read_text())['slots']
    selected = [s for s in entries if s['slug'] == slug and s['route'] == route]
    if len(selected) != 1:
        raise ValueError('slug/route is not a unique prepared slot')
    return selected[0]

def fallback_eligible(slug):
    primary = slot(slug, 'minimax')['run_id']
    reports = list((BUILD / 'n8-final-reports' / primary).glob('v0.10.*-mission-harness-deep.json'))
    if len(reports) != 1:
        return False
    data = json.loads(reports[0].read_text())
    scenarios = (data.get('live_gateway_scenarios') or {}).get('scenarios') or [{}]
    events = ((scenarios[0].get('failure_diagnostics') or {}).get('timeline') or {}).get('events') or []
    outcomes = [(event.get('payload') or {}) for event in events if event.get('kind') == 'context.provider_attempt_outcome']
    completed = sum(p.get('terminal_status') == 'completed' for p in outcomes)
    limited = sum(str(p.get('terminal_status', '')).lower() not in ('completed', '') and
                  ('429' in str(p.get('terminal_status')) or 'rate' in str(p.get('terminal_status')).lower() or
                   'too_many' in str(p.get('terminal_status')).lower()) for p in outcomes)
    return limited >= 2 and completed == 0

def verify_resources_absent(rid, unit):
    if (EVIDENCE / (rid + '-preregistration.json')).exists():
        raise ValueError('prereg already exists; refuse same-ID dispatch')
    state = run('systemctl', '--user', 'show', unit + '.service', '-p', 'LoadState', '--value')
    if state != 'not-found':
        raise ValueError('transient unit already exists: ' + unit)
    prefix = 'cowd-test-' + rid[:24] + '-'
    containers = run('docker', 'ps', '-a', '--format', '{{.Names}}\t{{.Labels}}').splitlines()
    volumes = run('docker', 'volume', 'ls', '--format', '{{.Name}}\t{{.Labels}}').splitlines()
    for line in containers + volumes:
        name, _, labels = line.partition('\t')
        if name.startswith(prefix) or ('cowd.test.run=' + rid) in labels.split(','):
            raise ValueError('existing owner container/volume: ' + name)

def private_dir(path):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError('private root must be an owned directory')
    try:
        path.chmod(0o700)
    except OSError as error:
        print(f'warning: private root chmod failed: {error}', file=sys.stderr)
    if stat.S_IMODE(path.lstat().st_mode) != 0o700:
        print('warning: private root mode is not 0700', file=sys.stderr)

def space_guard(*paths):
    def low():
        result = []
        for path in paths:
            path = Path(path)
            while not path.exists() and path != path.parent:
                path = path.parent
            try:
                st = os.statvfs(path)
                if st.f_bavail * st.f_frsize < 250000 * 1024:
                    result.append(str(path))
            except OSError as error:
                print(f'warning: cannot inspect disk {path}: {error}', file=sys.stderr)
        return result
    if low():
        reclaim = HERE.parents[0] / 'test' / 'reclaim.sh'
        try:
            subprocess.run(['bash', str(reclaim)], check=False)
        except OSError as error:
            print(f'warning: reclaim unavailable: {error}', file=sys.stderr)
        remaining = low()
        if remaining:
            print('warning: insufficient space after reclaim: ' + ', '.join(remaining), file=sys.stderr)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('action', choices=['check', 'prestage', 'launch'])
    ap.add_argument('--slug', required=True)
    ap.add_argument('--route', choices=['minimax', 'qwen'], default='minimax')
    args = ap.parse_args()
    s = slot(args.slug, args.route)
    ident = identity()
    perform(args, s, ident)

def perform(args, s, ident):
    rid = s['run_id']
    root = Path(s['private_root'])
    sidecar = Path(s['sidecar'] if args.route == 'minimax' else s['qwen_sidecar'])
    tree = TREES / ('n8-eval-' + rid)
    report = BUILD / 'n8-final-reports' / rid
    target = BUILD / 'n8-final-run-targets' / rid
    unit = 'cowd-034-' + args.slug + '-' + ('b' if args.route == 'minimax' else 'q')
    space_guard(BUILD, root, EVIDENCE, '/dev/shm', '/')
    verify_resources_absent(rid, unit)
    if args.action == 'check':
        print('identity, space, prereg, unit and owner resources checked:', rid)
        return
    if args.route == 'qwen' and not fallback_eligible(args.slug):
        raise ValueError('original 032 429 fallback condition not evidenced')
    if args.action == 'prestage':
        if any(os.path.lexists(path) for path in (root, tree, report, target)):
            raise ValueError('private root, eval worktree, report or target exists; refuse overwrite')
        cmd = [sys.executable, str(BUILD / 'b-scenario-launch.py'), '--run-id', rid,
               '--scenario', s['scenario'], '--head', ident['commit'], '--worktree', ident['host_worktree'],
               '--cowd', ident['cowd_path'], '--harness', ident['harness_path'],
               '--source-sha', ident['source_sha256'], '--version', ident['version']]
        if s.get('fixture') in ('u7', 'u8'):
            cmd += ['--' + s['fixture'] + '-fixture', str(root / (s['fixture'] + '-fixture.json'))]
        subprocess.run(cmd, check=True)
        private_dir(root)
        subprocess.run([sys.executable, str(HERE / (args.route + '-route.py')), '--run-id', rid], check=True)
        subprocess.run([sys.executable, str(HERE / (args.route + '-route.py')), '--apply', '--run-id', rid], check=True)
        if not sidecar.is_file() or sidecar.stat().st_mode & 0o777 != 0o600:
            raise ValueError('sidecar missing or not 0600')
        print('PRESTAGED:', rid)
        return
    private_dir(root)
    for path in (report, target):
        if not path.is_dir() or any(path.iterdir()):
            raise ValueError('prepared report/target missing or nonempty; refuse append')
    sidecar_info = sidecar.lstat()
    if not stat.S_ISREG(sidecar_info.st_mode) or sidecar_info.st_uid != os.getuid():
        raise ValueError('sidecar is not a regular file owned by caller')
    cfg = json.loads(sidecar.read_text())
    expected_model = 'MiniMax-M3.1-Flash' if args.route == 'minimax' else 'qwen3.8-flash'
    expected_route = '/run/user/1000/' + ('cowd-b-minimax-route' if args.route == 'minimax' else 'cowd-b-qwen-tokenplan-route')
    expected = {'run_id': rid, 'scenario': s['scenario'], 'head': ident['commit'], 'source_sha': ident['source_sha256'],
                'cowd_bin': ident['cowd_path'], 'harness_bin': ident['harness_path'], 'worktree': ident['host_worktree'],
                'candidate_tree': str(tree), 'private_root': str(root), 'slot': True,
                'model': expected_model, 'route_src': expected_route}
    if (sidecar.stat().st_mode & 0o777 != 0o600 or cfg.get('timeout_secs', 1800) != 1800 or
            any(cfg.get(k) != v for k, v in expected.items())):
        raise ValueError('sidecar differs from original candidate or route')
    if run('git', '-C', str(tree), 'rev-parse', 'HEAD') != ident['commit']:
        raise ValueError('eval worktree differs from original candidate')
    round_group = run('systemctl', '--user', 'show', 'cowd-hub-round.service', '-p', 'ControlGroup', '--value')
    if not round_group:
        raise ValueError('round cgroup unavailable; refuse dispatch')
    space_guard(BUILD, root, EVIDENCE, '/dev/shm', '/')
    verify_resources_absent(rid, unit)
    subprocess.run(['systemd-run', '--user', '--unit=' + unit,
                    '--property=StandardOutput=append:' + str(report / 'controller.stdout.log'),
                    '--property=StandardError=append:' + str(report / 'controller.stderr.log'),
                    'env', '-u', 'COWD_LAUNCHER_HANDOFF_BIN', sys.executable,
                    str(BUILD / 'b-n8-controller/controller.py'), str(sidecar)], check=True)
    (report / 'candidate-source-observation.json').write_text(json.dumps({
        'head': ident['commit'], 'diff_sha256': ident['diff_sha256'],
    }, indent=2) + '\n')
    group = run('systemctl', '--user', 'show', unit + '.service', '-p', 'ControlGroup', '--value')
    print('UNIT', unit, 'CGROUP', group, 'ROUND', round_group)
    if not group or not round_group or group == round_group:
        raise ValueError('unit isolation not confirmed; stop further dispatch and inspect the running unit')

if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print('HOLD:', error, file=sys.stderr)
        raise SystemExit(2)
