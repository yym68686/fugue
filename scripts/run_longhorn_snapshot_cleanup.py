"""Fugue's online Longhorn cleanup queue and bounded native executor."""
import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import threading
import time
import urllib.parse

from scripts.plan_longhorn_snapshot_recovery import NAME, require
from scripts.reconcile_longhorn_snapshot_cleanup import (
    Cluster, candidate_ledger, database_binding, reconcile, snapshot_key, validate_policy,
)
from scripts.recover_longhorn_snapshot import canonical, timestamp

WATCH_LABEL = 'fugue.pro/backup-run'
PREVIEW_READY = 'preview ready; execution not authorized in observe mode'


def valid_key(key):
    return (len(key) == 3 and all(isinstance(v, str) for v in key) and
            NAME.fullmatch(key[0]) and NAME.fullmatch(key[1]) and re.fullmatch(r'[a-zA-Z0-9_-]{1,200}', key[2]))


def backup_tombstone(event):
    obj = event.get('object') or {}
    if event.get('type') != 'DELETED' or obj.get('kind') != 'Backup':
        return None
    meta, spec, status = (obj.get(k) or {} for k in ('metadata', 'spec', 'status'))
    if not meta.get('deletionTimestamp'):
        return None
    key = (status.get('volumeName'), spec.get('snapshotName'), (meta.get('labels') or {}).get(WATCH_LABEL))
    return key if valid_key(key) else None


def emit(phase, **fields):
    print(canonical(dict(phase=phase, observed_at=timestamp(), **fields)), flush=True)


class Queue:
    def __init__(self, path):
        self.path = path
        with self.transaction() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS jobs (
                volume TEXT NOT NULL, snapshot TEXT NOT NULL, run TEXT NOT NULL,
                source TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                reason TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0,
                due INTEGER NOT NULL, PRIMARY KEY(volume,snapshot,run))''')
            db.execute('CREATE INDEX IF NOT EXISTS jobs_due ON jobs(state,due)')

    @contextlib.contextmanager
    def transaction(self):
        with contextlib.closing(sqlite3.connect(self.path, timeout=5)) as db, db:
            yield db

    def get(self, name):
        with self.transaction() as db:
            row = db.execute('SELECT value FROM metadata WHERE name=?', (name,)).fetchone()
        return None if row is None else row[0]

    @staticmethod
    def put(db, name, value):
        db.execute('INSERT INTO metadata VALUES (?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value', (name, value))

    @staticmethod
    def enqueue(db, key, source, now):
        require(valid_key(key), 'invalid queue identity')
        cursor = db.execute('INSERT OR IGNORE INTO jobs(volume,snapshot,run,source,due) VALUES (?,?,?,?,?)',
                            (*key, source, now))
        return cursor.rowcount == 1

    def checkpoint(self, version, key=None):
        with self.transaction() as db:
            inserted = self.enqueue(db, key, 'remote-delete-event', int(time.time())) if key else False
            self.put(db, 'remote-backup-resource-version', version)
        return inserted

    def refill(self, keys, now):
        with self.transaction() as db:
            return sum(self.enqueue(db, key, 'inventory', now) for key in keys)

    def due(self, now):
        with self.transaction() as db:
            return db.execute('''SELECT volume,snapshot,run,attempts FROM jobs
                WHERE state IN ('pending','retry') AND due<=? ORDER BY due,volume,snapshot LIMIT 1''', (now,)).fetchone()

    def record(self, key, state, reason, due, increment=1):
        with self.transaction() as db:
            db.execute('UPDATE jobs SET state=?,reason=?,due=?,attempts=attempts+? WHERE volume=? AND snapshot=? AND run=?',
                       (state, str(reason)[:512], due, increment, *key))

    def recheck_gone(self, keys, now):
        # If a previously absent identity returns it still needs complete live admission.
        with self.transaction() as db:
            for key in keys:
                db.execute("UPDATE jobs SET state='retry',due=? WHERE state='absent' AND (volume,snapshot,run)=(?,?,?)",
                           (now, *key))

    def activate(self, now):
        with self.transaction() as db:
            db.execute("UPDATE jobs SET due=? WHERE state='retry' AND reason=? AND due>?", (now, PREVIEW_READY, now))

    def count(self):
        with self.transaction() as db:
            return dict(db.execute('SELECT state,count(*) FROM jobs GROUP BY state').fetchall())


class RemoteWatch:
    def __init__(self, kube, namespace, queue, stop, ready):
        self.kube, self.queue, self.stop, self.ready = kube, queue, stop, ready
        self.backfill = threading.Event()
        self.path = '/apis/longhorn.io/v1beta2/namespaces/' + namespace + '/backups'

    def list_version(self):
        query = urllib.parse.urlencode(dict(labelSelector=WATCH_LABEL, limit=1))
        obj = json.loads(self.kube.command(['get', '--raw', self.path + '?' + query]))
        version = obj['metadata']['resourceVersion']
        require(isinstance(version, str) and version, 'remote inventory has no resource version')
        return version

    def accept(self, raw):
        event = json.loads(raw)
        obj = event.get('object') or {}
        if event.get('type') == 'ERROR':
            if obj.get('code') == 410:
                return False
            raise ValueError('remote watch returned an error')
        version = (obj.get('metadata') or {}).get('resourceVersion')
        require(event.get('type') in ('ADDED','MODIFIED','DELETED','BOOKMARK') and
                isinstance(version, str) and version, 'invalid remote watch event')
        key = backup_tombstone(event)
        if self.queue.checkpoint(version, key):
            emit('enqueued', source='remote-delete-event', volume=key[0], snapshot=key[1], run=key[2])
        return True

    def reset(self):
        self.ready.clear()
        self.backfill.set()
        self.queue.checkpoint(self.list_version())

    def watch(self, version):
        query = urllib.parse.urlencode(dict(watch='true', labelSelector=WATCH_LABEL, allowWatchBookmarks='true',
                                            resourceVersion=version, timeoutSeconds=50))
        args = self.kube.kubectl + ['--request-timeout=65s', 'get', '--raw', self.path + '?' + query]
        with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1) as process:
            try:
                while line := process.stdout.readline((2 << 20) + 1):
                    if self.stop.is_set():
                        return
                    require(len(line) <= (2 << 20), 'remote watch event exceeds bound')
                    if not self.accept(line):
                        self.reset()
                        return
                    self.ready.set()
                if process.wait(timeout=5) != 0:
                    # An expired resourceVersion can be returned as an HTTP
                    # error before a watch stream starts. Backfill covers it.
                    self.reset()
                    emit('watch-reconnect', reason='watch closed with an HTTP or transport error')
                    self.stop.wait(5)
                else:
                    # A quiet stream can close normally without any events.
                    self.ready.set()
            finally:
                if process.poll() is None:
                    process.terminate()

    def run(self):
        backoff = 2
        while not self.stop.is_set():
            try:
                version = self.queue.get('remote-backup-resource-version')
                if not version:
                    version = self.list_version()
                    self.queue.checkpoint(version)
                self.watch(version)
                backoff = 2
            except (OSError, KeyError, ValueError, subprocess.SubprocessError, sqlite3.Error) as error:
                self.ready.clear()
                emit('watch-retry', reason=str(error)[:256], retry_seconds=backoff)
                self.stop.wait(backoff)
                backoff = min(backoff * 2, 60)


def inventory(kube, policy):
    """Bounded, read-only catch-up; normally remote tombstones wake the queue."""
    base = '/apis/longhorn.io/v1beta2/namespaces/' + policy['longhornNamespace']
    items = kube.items(base + '/snapshots', labelSelector=WATCH_LABEL)
    platform = database_binding(kube, policy, policy['namespace'], policy['cluster'])
    cache = {}
    found = []
    for snapshot in items:
        key = snapshot_key(snapshot)
        if not valid_key(key):
            continue
        volume, name, run = key
        if volume not in cache:
            require(len(cache) < 32, 'cleanup inventory exceeds volume budget')
            cache[volume] = kube.volume_ledger({**platform, 'volume': volume})
        rows = cache[volume]
        if not any(name in (r.get('snapshot'),r.get('alternate_snapshot')) for r in rows):
            rows.extend(kube.failed_snapshot_run({**platform, 'run':run, 'snapshot':name}))
        try:
            candidate_ledger(rows, name, run)
        except ValueError:
            continue
        found.append(key)
    return found


def reconcile_job(queue, row, result, now, active):
    """Record what actually happened; native completion is never guessed."""
    selected = tuple(result.get('selected_target', ()))
    phase = result['phase']
    if active and phase == 'completed' and valid_key(selected):
        queue.record(selected, 'completed', 'native completion verified', 0, 0)
    if row is None or (active and phase == 'completed' and selected == tuple(row[:3])):
        return
    key, attempts = tuple(row[:3]), row[3]
    if selected and selected != key:
        queue.record(key, 'retry', 'another native operation must reach a verified safe state', now + 15, 0)
        return
    if key not in [tuple(item) for item in result.get('observed_targets', [])] and phase == 'idle':
        queue.record(key, 'absent', 'snapshot not in live inventory; no native completion receipt', 0)
        return
    if phase == 'ready' and not active:
        queue.record(key, 'retry', PREVIEW_READY, now + 600, 0)
        return
    reason = next((b['reason'] for b in result.get('blocked', []) if b.get('snapshot', key[1]) == key[1]),
                  next((b['reason'] for b in result.get('retained', []) if b.get('snapshot') == key[1]), phase))
    in_progress = phase in ('pending-native-operation', 'native-running')
    queue.record(key, 'retry', reason, now + (15 if in_progress else min(1800, 30 * 2**min(attempts, 6))), int(not in_progress))


def save_atomic(path, value):
    partial = path.with_suffix('.tmp')
    partial.write_text(canonical(value) + '\n')
    partial.replace(path)


def run(kube, policy, directory, revision, mode, once=False):
    validate_policy(policy)
    require(re.fullmatch(r'[0-9a-f]{40}', revision) and mode in ('observe','active'), 'invalid executor release identity')
    active = mode == 'active'
    queue = Queue(directory / 'queue.sqlite3')
    if active:
        queue.activate(int(time.time()))
    stop, watch_ready = threading.Event(), threading.Event()
    watch = RemoteWatch(kube, policy['longhornNamespace'], queue, stop, watch_ready)
    if not once:
        threading.Thread(target=watch.run, daemon=True).start()
    refill_at, inspect_at, checked_at, last_refill = 0, 0, 0, 0
    policy_digest = hashlib.sha256(canonical(policy).encode()).hexdigest()
    status = {'phase': 'starting'}
    last_heartbeat, last_watch = 0, None

    def report(now, watch_connected):
        outcome = dict(status, mode=mode, revision=revision, observed_at=timestamp(),
                       watch_bootstrapped=watch_connected, inventory_verified=checked_at > 0 and now - checked_at < 1200,
                       policy_sha256=policy_digest, queue=queue.count())
        save_atomic(directory / 'status.json', outcome)
        return outcome

    try:
        while True:
            now = int(time.time())
            if now >= refill_at or (watch.backfill.is_set() and now >= last_refill + 60):
                watch.backfill.clear()
                last_refill = now
                try:
                    found = inventory(kube, policy) if policy['enabled'] else []
                    new = queue.refill(found, now)
                    queue.recheck_gone(found, now)
                    checked_at = int(time.time())
                    emit('inventory', candidates=len(found), enqueued=new, mode=mode, revision=revision)
                    refill_at = now + 900
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    emit('inventory-retry', reason=str(error)[:256])
                    refill_at = now + 60
                    if once:
                        raise
            watching = watch_ready.is_set()
            if watching != last_watch or now >= last_heartbeat + 30:
                report(now, watching)
                last_heartbeat, last_watch = now, watching
            # Require a healthy event and inventory path at startup before
            # admitting new work. Resume durable native protection even if
            # event transport is temporarily unavailable.
            row = queue.due(now) if not active or (checked_at and watching) else None
            if row or now >= inspect_at:
                try:
                    result = reconcile(kube, policy, directory / 'native.json', execute=active,
                                       targets={tuple(row[:3])} if row else set())
                    ended = int(time.time())
                    reconcile_job(queue, row, result, ended, active)
                    inspect_at = ended + (15 if result['phase'] in ('pending-native-operation','native-running') else 300)
                    status = {k:result[k] for k in ('phase','selected_snapshot','blocked','retained','operation','completed','budget')
                              if k in result}
                    status['target'] = list(row[:3]) if row else None
                    emit('reconcile', result=report(ended, watch_ready.is_set()))
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    now = int(time.time())
                    if row:
                        queue.record(tuple(row[:3]), 'retry', str(error), now + min(1800, 30 * 2**min(row[3],6)))
                    inspect_at = now + 30
                    status = dict(phase='retry', reason=str(error)[:256], target=list(row[:3]) if row else None)
                    report(now, watch_ready.is_set())
                    emit('reconcile-retry', reason=str(error)[:256], target=list(row[:3]) if row else None)
                    if once:
                        raise
            if once:
                return
            stop.wait(5)
    finally:
        stop.set()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('policy', type=Path)
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--mode', choices=['observe','active'], required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    require(not args.once or args.mode == 'observe', 'one shot is preview only')
    run(Cluster(), json.loads(args.policy.read_text()), args.state, args.revision, args.mode, args.once)


if __name__ == '__main__':
    main()
