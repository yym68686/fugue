#!/usr/bin/env python3
"""Disposable PostgreSQL crash/copy/replay rehearsal; run inside an isolated container.

docker run --rm --network none --user 26:26 --entrypoint python3 \
  -v "$PWD:/fugue:ro" ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie \
  /fugue/scripts/tests/cold_postgres_rehearsal.py
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def run(*args):
    return subprocess.check_output(args, stderr=subprocess.STDOUT, text=True)


def evidence(data):
    return json.loads(run('python3', '/fugue/internal/controller/postgres_cold_probe.py', 'seed', str(data)))


with tempfile.TemporaryDirectory(prefix='fugue-cold-rehearsal-') as root:
    root = Path(root)
    source, target = root/'source', root/'target'
    source_socket, target_socket = root/'source-socket', root/'target-socket'
    source_socket.mkdir(); target_socket.mkdir()
    run('initdb', '-D', str(source), '-A', 'trust', '--no-locale')
    run('pg_ctl', '-D', str(source), '-l', str(root/'source.log'), '-o', f'-k {source_socket} -p 6543', '-w', 'start')
    try:
        run('psql', '-h', str(source_socket), '-p', '6543', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c',
            'CREATE TABLE durable_rows(id integer primary key, payload text); CHECKPOINT; INSERT INTO durable_rows SELECT i, md5(i::text) FROM generate_series(1,1000) i;')
    finally:
        # Deliberately leave the data directory requiring crash recovery.
        run('pg_ctl', '-D', str(source), '-m', 'immediate', '-w', 'stop')
    control = run('pg_controldata', str(source))
    assert 'in production' in control, control
    before = evidence(source)
    target.mkdir()
    producer = subprocess.Popen(['tar','-cpf','-','-C',str(source),'.'],stdout=subprocess.PIPE)
    subprocess.run(['tar','-xpf','-','-C',str(target)],stdin=producer.stdout,check=True)
    producer.stdout.close()
    assert producer.wait() == 0
    copied = evidence(target)
    for field in ['system_id','file_digest','control_digest','files','bytes']:
        assert before[field] == copied[field], field
    # A silent bit flip must be detected before activation.
    wal = next(p for p in (target/'pg_wal').iterdir() if p.is_file())
    with open(wal,'r+b') as f:
        original=f.read(1);f.seek(0);f.write(bytes([original[0]^1]))
    assert evidence(target)['file_digest'] != before['file_digest']
    with open(wal,'r+b') as f:f.write(original)
    assert evidence(target)['file_digest'] == before['file_digest']
    run('pg_ctl','-D',str(target),'-l',str(root/'target.log'),'-o',f'-k {target_socket} -p 6544','-w','start')
    try:
        row=run('psql','-h',str(target_socket),'-p','6544','-d','postgres','-At','-c',
                'SELECT count(*),sum(id),bool_and(payload=md5(id::text)) FROM durable_rows;').strip()
        assert row == '1000|500500|t',row
        system_id=run('psql','-h',str(target_socket),'-p','6544','-d','postgres','-At','-c',
                      'SELECT system_identifier FROM pg_control_system();').strip()
        assert system_id == before['system_id']
        print(json.dumps({'result':'passed','source_state':'unclean shutdown','source_untouched':evidence(source)==before,
                          'copied_files':before['files'],'corruption_detected':True,'replayed_rows':1000,'matching_system_id':True}))
    finally:run('pg_ctl','-D',str(target),'-m','fast','-w','stop')
