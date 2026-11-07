#!/usr/bin/env python3
"""One non-replayable index page plus one coherent inventory attempt.

Both existing executors keep their original gates and receipts. This trusted
coordinator adds a single cumulative baseline and continuous 4GiB backing
reserve; a successful page never refreshes the space/WAL/writer allowance.
No operation declaration is shipped with this capability.
"""
import argparse
import copy
import datetime
import hashlib
import json
import os
from pathlib import Path

from scripts import compiler_input_hot_index as index
from scripts import compiler_input_copy as impact
from scripts import compiler_input_readonly as readonly
from scripts.compiler_input_storage import command, kube

EXTRA_RESERVE = 4 << 30
EVIDENCE_LIMIT = 32 << 20
PARENT_EVIDENCE_LIMIT = 2 << 20
MAINTENANCE_PURPOSES = (index.PURPOSE, readonly.PURPOSE, impact.PURPOSE,
                        'compiler-input-task-prepare', 'compiler-input-publication-prepare', 'compiler-input-consumer-prepare')
require = readonly.require


def maintenance_gate(c, existing_job=''):
    # The two children may each own one Job, but a concurrent maintenance
    # worker must not consume the same cumulative allowance between samples.
    observed = []
    for purpose in MAINTENANCE_PURPOSES:
        jobs = json.loads(kube(c['namespace'], 'get', 'jobs', '-l', 'fugue.pro/purpose='+purpose, '-o', 'json'))['items']
        require(all(job['metadata']['uid'] == existing_job for job in jobs),
                'stage-one concurrent maintenance resource appeared')
        observed.extend(job['metadata']['uid'] for job in jobs)
    require(len(observed) <= 1, 'stage-one maintenance population changed')


def validate(c):
    require(type(c) is dict and set(c) == {'version', 'executionAuthorized', 'index', 'inventory'}
            and type(c['version']) is int and c['version'] == 1 and c['executionAuthorized'] is True,
            'stage-one declaration differs')
    a, b = index.validate(c['index']), readonly.validate(c['inventory'])
    require(a['page']['limit'] == 1 and b['version'] == 3 and b['readCount'] == 0,
            'stage one permits only one index item and one metadata inventory')
    require(all(a[k] == b[k] for k in index.COMMON-{'version'})
            and b['minimumFreeBytes'] >= 3 << 30, 'stage-one identities or reserves differ')
    return c


def check_backing(value, starting=False, drop=128 << 20):
    # Recompute with the observed total on every call; never add independent
    # disks or substitute f_bavail for the existing f_bfree contract.
    require(type(value) is dict and all(type(value.get(k)) is int and value[k] >= 0
            for k in ('total_bytes', 'free_bytes', 'available_bytes'))
            and 0 <= value['available_bytes'] <= value['free_bytes'] <= value['total_bytes']
            and value['total_bytes'] > 0, 'stage-one backing observation malformed')
    required = value['total_bytes']//4+(16 << 30)+(192 << 20)+(256 << 20)+EXTRA_RESERVE
    if starting:
        required += drop
    require(value.get('capacity_gate_passed') is True and value['free_bytes'] >= required,
            'stage-one continuous backing reserve unavailable')
    return {**value, 'stage_required_bytes': required, 'extra_continuous_reserve_bytes': EXTRA_RESERVE}


class Window:
    def __init__(self, c, save):
        self.c, self.save = c, save
        self.registry = json.loads(Path('deploy/releases/components.json').read_text())
        impact.node_gate(c)
        _, primary, self.available = readonly.primary_gate(c)
        backing = check_backing(impact.backing_gate(c, primary, starting=True), True, c['maxAvailableDropBytes'])
        self.restarts = {s['name']: s['restartCount'] for s in primary['status']['containerStatuses']}
        _, _, self.serving = readonly.serving_gate(c, self.registry)
        self.metadata = index.metadata(c)
        require(self.metadata['worker_connections'] == 0, 'stage-one index worker already exists')
        self.writers = impact.api_activity(c, self.serving)
        self.baseline = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                         'available_bytes': self.available, 'metadata': copy.deepcopy(self.metadata),
                         'writers': copy.deepcopy(self.writers), 'serving': copy.deepcopy(self.serving), 'backing': backing}
        self.samples = []

    def check(self, existing_job=''):
        require(len(self.samples) < 50, 'stage-one cumulative watchdog exhausted')
        maintenance_gate(self.c, existing_job)
        _, primary, available = readonly.primary_gate(self.c)
        require({s['name']: s['restartCount'] for s in primary['status']['containerStatuses']} == self.restarts
                and self.available-available <= self.c['maxAvailableDropBytes'],
                'stage-one cumulative space or restart budget exceeded')
        backing = check_backing(impact.backing_gate(self.c, primary))
        _, _, serving = readonly.serving_gate(self.c, self.registry)
        require(serving == self.serving, 'stage-one runtime changed')
        now = index.metadata(self.c)
        index.check_metadata(self.c, self.metadata, now)
        writers = impact.api_activity(self.c, self.serving)
        require(set(writers) == set(self.writers), 'stage-one writer population changed')
        for uid, value in writers.items():
            old = self.writers[uid]
            impact.check_writer_impact(self.c, old, value)
            require(value['budget_fallbacks'] == old['budget_fallbacks']
                    and 0 <= value['write_failures']-old['write_failures'] <= self.c['maxNewDeadlineFailures'],
                    'stage-one cumulative inner writer budget exceeded')
        sample = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'available_bytes': available,
                  'metadata': now, 'writers': writers, 'backing': backing,
                  'node': impact.node_gate(self.c, existing_job),
                  'bounded_logs': impact.api_log_signals(self.c, self.serving)}
        self.samples.append(sample)
        self.save(self.baseline, self.samples)


def intent(c, name, digest):
    item = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'immutable': True,
            'metadata': {'namespace': c['namespace'], 'name': name},
            'data': {'declaration-sha256': digest, 'intent': 'one-index-page-one-inventory-no-retry'}}
    created = readonly.create(item)
    observed = readonly.get(c['namespace'], 'configmap', name)
    require(created['metadata']['uid'] == observed['metadata']['uid'] and observed.get('immutable') is True
            and observed.get('data') == item['data'], 'stage-one intent readback differs')
    return created['metadata']['uid']


def terminal_receipt(c, name, report, paths):
    # Persist small exact file digests, not the potentially multi-MiB graph.
    # Both successful and uncertain attempts consume the same immutable intent.
    files = {}
    for path in paths:
        if path.exists():
            digest = hashlib.sha256()
            with path.open('rb') as handle:
                for block in iter(lambda: handle.read(65536), b''):
                    digest.update(block)
            files[path.name] = {'sha256': digest.hexdigest(), 'bytes': path.stat().st_size}
    value = {k: report[k] for k in ('schema', 'declaration_sha256', 'status', 'cleanup_confirmed',
             'source_preserved', 'retirement_authorized', 'extra_continuous_reserve_bytes', 'finished_at')}
    value.update(files=files, index=report.get('index'), inventory=report.get('inventory'))
    value.update(intent_uid=report.get('intent_uid'),
                 window_sha256=readonly.identity({k: report.get(k) for k in ('baseline', 'impact_samples')}),
                 impact_sample_count=len(report.get('impact_samples', [])))
    for key in ('error_type', 'refusal_reason'):
        if key in report:
            value[key] = report[key]
    body = json.dumps(value, separators=(',', ':'))
    require(len(body.encode()) <= 32768, 'stage-one terminal receipt exceeds budget')
    item = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'immutable': True,
            'metadata': {'namespace': c['namespace'], 'name': name},
            'data': {'declaration-sha256': report['declaration_sha256'], 'receipt.json': body}}
    created = readonly.create(item)
    observed = readonly.get(c['namespace'], 'configmap', name)
    require(created['metadata']['uid'] == observed['metadata']['uid'] and observed.get('immutable') is True
            and observed.get('data') == item['data'], 'stage-one terminal receipt readback differs')
    return {'uid': created['metadata']['uid'], 'confirmed': True}


def execute(c, evidence, apply=False):
    validate(c)
    a, b = c['index'], c['inventory']
    digest = readonly.identity(c)
    name = 'fugue-input-stage1-'+digest[:20]
    index_path = evidence.with_name(evidence.stem+'-index.json')
    inventory_path = evidence.with_name(evidence.stem+'-inventory.json')
    report = {'schema': 'fugue.compiler-input-stage1.v1', 'declaration_sha256': digest,
              'status': 'preflight', 'retirement_authorized': False, 'source_preserved': True,
              'extra_continuous_reserve_bytes': EXTRA_RESERVE, 'cleanup_confirmed': False}
    paths = (evidence, index_path, inventory_path)
    def save():
        body = json.dumps(report, separators=(',', ':'))+'\n'
        require(len(body.encode()) <= PARENT_EVIDENCE_LIMIT, 'stage-one parent evidence budget exceeded')
        require(len(body.encode())+sum(p.stat().st_size for p in paths[1:] if p.exists()) <= EVIDENCE_LIMIT,
                'stage-one evidence budget exceeded')
        evidence.write_text(body)
    def save_window(baseline, samples):
        report.update(baseline=baseline, impact_samples=samples)
        save()
    with evidence.open('x') as f:
        f.write(json.dumps(report)+'\n')
    consumed = False
    try:
        require(os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REF') == 'refs/heads/main'
                and os.environ.get('GITHUB_SHA') == command(['git', 'rev-parse', 'HEAD']).strip(),
                'stage one requires exact main CI checkout')
        require(not index_path.exists() and not inventory_path.exists(), 'stage-one child evidence already exists')
        maintenance_gate(a)
        window = Window(a, save_window)
        save_window(window.baseline, window.samples)
        if apply:
            consumed = True
            report['intent_uid'] = intent(a, name+'-receipt', digest)
            save()
        # Both children enforce their remaining on-disk allowance before
        # writes. The parent's check alone would discover an oversized child
        # only after the cumulative evidence limit had already been consumed.
        index_limit = EVIDENCE_LIMIT-PARENT_EVIDENCE_LIMIT
        require(index_limit >= 16384, 'stage-one index evidence headroom unavailable')
        page = index.execute(a, index_path, apply, cumulative_watch=window.check, evidence_limit=index_limit)
        report['index'] = {k: page[k] for k in ('status', 'cleanup_confirmed', 'job', 'job_uid',
                           'declaration_sha256', 'terminal_receipt') if k in page}
        if not apply:
            require(page['status'] == 'preview-passed', 'stage-one index preview incomplete')
        else:
            require(page['status'] == 'index-page-committed' and page['cleanup_confirmed']
                    and page.get('terminal_receipt', {}).get('confirmed') is True,
                    'stage-one page or cleanup unconfirmed')
            require(page['result']['source_bytes'] <= 8 << 20
                    and page['result']['index_bytes'] <= (8 << 20)+(16 << 10), 'stage-one page exceeds item budget')
        window.check()  # Same baseline, including the gap between executors.
        # Leave room for structured result/evidence wrappers and final samples.
        # Refuse before collecting logs if the cumulative local allowance is
        # already exhausted; never fetch 64MiB then discover a 32MiB limit.
        evidence_limit = EVIDENCE_LIMIT-PARENT_EVIDENCE_LIMIT-sum(p.stat().st_size for p in paths[1:] if p.exists())
        output_limit = min(8 << 20, evidence_limit//4)
        require(output_limit >= 16384, 'stage-one inventory evidence headroom unavailable')
        inventory = readonly.execute(b, inventory_path, apply, cumulative_watch=window.check,
                                     output_limit=output_limit, evidence_limit=evidence_limit)
        report['inventory'] = {k: inventory[k] for k in ('status', 'cleanup_confirmed', 'job', 'job_uid',
                               'declaration_sha256') if k in inventory}
        if apply:
            require(inventory['status'] == 'diagnostic-finished' and inventory['cleanup_confirmed'],
                    'stage-one inventory or cleanup unconfirmed')
            terminal = inventory['execution']['terminal']
            complete = inventory['result']['graph_command_completed']
            require(terminal['metadata_complete'] and terminal['signal'] == 0
                    and (terminal['exit_code'] == 0 if complete else terminal['exit_code'] != 0),
                    'stage-one inventory termination differs from bounded result')
            # A coherent, structured failure is an attempted inventory, never
            # complete H. It consumes this window and cannot trigger a retry.
            report['inventory']['graph_command_completed'] = complete
            report['cleanup_confirmed'] = True
        window.check()
        report['status'] = 'stage-attempt-finished' if apply else 'preview-passed'
        save()
        return report
    except Exception as error:
        report.update(status='completion-not-confirmed', error_type=type(error).__name__, intent_consumed=consumed)
        if isinstance(error, readonly.Refusal):
            report['refusal_reason'] = str(error)
        # Existing child executors retain exact-owned termination/cleanup and
        # immutable receipts. An uncertain result must not launch the next step.
        raise
    finally:
        report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        evidence_error = None
        try:
            save()
        except Exception as error:
            # A local artifact limit or I/O failure must not suppress the
            # independent immutable terminal receipt for a consumed attempt.
            evidence_error = error
            report.update(status='completion-not-confirmed', error_type=type(error).__name__)
        if consumed:
            try:
                report['terminal_receipt'] = terminal_receipt(a, name+'-terminal', report, paths[1:])
            except Exception as error:
                report['terminal_receipt'] = {'confirmed': False, 'error_type': type(error).__name__}
            try:
                save()
            except Exception as error:
                evidence_error = error
        if evidence_error is not None:
            raise evidence_error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('declaration')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    try:
        from scripts.compiler_input_copy_resume import strict_json
        c = validate(strict_json(Path(args.declaration).read_bytes()))
        result = execute(c, args.evidence, args.apply)
        if args.apply:
            require(result.get('terminal_receipt', {}).get('confirmed') is True, 'stage-one terminal unconfirmed')
        print(json.dumps({k: result[k] for k in ('status', 'cleanup_confirmed', 'retirement_authorized')}))
    except Exception as error:
        raise SystemExit('stage-one completion not confirmed; inspect evidence; category='+type(error).__name__) from None


if __name__ == '__main__':
    main()
