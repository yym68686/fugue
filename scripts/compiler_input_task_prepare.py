#!/usr/bin/env python3
"""One receipted task-input restoration, never task activation or retirement.

The exact serving API signs and confirms over bounded stdin. Only a public
ticket, an ordinary DB session and the dedicated GET role enter the isolated
worker. An intent is consumed before credentials or a worker are created; an
uncertain attempt is not replayed. No operation declaration means no work.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from scripts import compiler_input_copy as impact
from scripts import compiler_input_hot_index as index
from scripts import compiler_input_readonly as r
from scripts.compiler_input_copy_resume import strict_json
from scripts.compiler_input_storage import command, get, kube

from scripts import compiler_input_prepare as shared
import sys

PURPOSE = 'compiler-input-task-prepare'
SUBJECT = 'task'
NAME_PREFIX = 'fugue-input-task-'
NATIVE_PREFIX = 'task-input'
VERIFICATION_ENV = 'FUGUE_TASK_INPUT_PREPARATION_VERIFICATION_KEY'
ACTIVATED_FIELD = 'task_activated'
SUCCESS = 'task-inputs-prepared'
CONTRACT_LIMIT = 16384
BOUNDS = shared.BOUNDS
require = r.require
instant = shared.instant

def validate(c):
    shared.validate_common(c, SUBJECT)
    t = c['task']
    require(type(t) is dict and set(t) == set(BOUNDS) | {'task_id', 'updater_id', 'operation_key'},
            'invalid task preparation budget')
    require(all(type(t[k]) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', t[k])
                for k in ('task_id', 'updater_id'))
            and type(t['operation_key']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:/-]{0,47}', t['operation_key']),
            'task preparation identity is not bounded')
    require(all(type(t[k]) is int and 1 <= t[k] <= high for k, high in BOUNDS.items()),
            'task preparation budget exceeds original envelope')
    return c


def contract(c):
    return {'version': 1, **c['task'], 'expected_plan_sha256': ''}


def physical_owner(c, v):
    require(v['table'] != 'fugue_platform_lkg_snapshots'
            or v['key'] == '["release_set","global"]', 'task configuration selector differs')
    return v['table'] == 'fugue_node_update_tasks' and v['key'] == c['task']['task_id']


def receipt_identity(c):
    return {'task_id': c['task']['task_id']}


def native(c, executor, mode, payload):
    return shared.native(c, executor, mode, payload, sys.modules[__name__])


def plan_contract(value, c):
    return shared.plan_contract(value, c, sys.modules[__name__])


def metadata(c, reader=False):
    return shared.metadata(c, sys.modules[__name__], reader)


def session(c, cluster, primary, deployment, executor):
    return shared.session(c, cluster, primary, deployment, executor, sys.modules[__name__])


def job_manifest(c, name, secret_name):
    return shared.job_manifest(c, name, secret_name, sys.modules[__name__])


def result_contract(value, c, plan, confirmed=False):
    return shared.result_contract(value, c, plan, sys.modules[__name__], confirmed)


def maintenance_gate(c, own_uid=''):
    return shared.maintenance_gate(c, own_uid, kube)


def execute(c, evidence, apply=False):
    return shared.execute(c, evidence, apply, sys.modules[__name__])


def terminal_receipt(c, report):
    return shared.terminal_receipt(c, report, sys.modules[__name__])


def main():
    shared.main(sys.modules[__name__])


if __name__ == '__main__':
    main()
