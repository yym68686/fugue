package api

// The policy is an explicit host task, not a side effect of installing a new
// updater. The resulting kubelet configuration survives code release failures.
func podCapacityShellLibrary() string {
	return `
reload_pod_capacity_k3s() {
  python3 - <<'FUGUE_CAPACITY_RELOAD_PY'
import subprocess, sys, time, urllib.request
deadline = time.monotonic() + 90
try:
    subprocess.run(['systemctl', 'restart', 'k3s-agent'], check=True, timeout=90)
    while time.monotonic() < deadline:
        active = subprocess.run(['systemctl', 'is-active', '--quiet', 'k3s-agent'], timeout=5).returncode == 0
        try:
            with urllib.request.urlopen('http://127.0.0.1:10248/healthz', timeout=3) as response:
                healthy = response.status == 200 and response.read(64).strip() == b'ok'
        except Exception:
            healthy = False
        if active and healthy:
            sys.exit(0)
        time.sleep(2)
    raise RuntimeError('kubelet did not become healthy within 90 seconds')
except Exception as error:
    print('capacity reload failed: ' + str(error), file=sys.stderr)
    sys.exit(1)
FUGUE_CAPACITY_RELOAD_PY
}

reconcile_pod_capacity_task() {
  local mode="${FUGUE_NODE_UPDATE_TASK_POD_CAPACITY_MODE:-}"
  local dry_run="${FUGUE_NODE_UPDATE_TASK_DRY_RUN:-true}"
  local config="${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
  local staged=""
  local backup=""
  case "${mode}" in resources|default) ;; *) echo "unsupported pod capacity mode" >&2; return 1 ;; esac
  if ! truthy "${dry_run}" && ! truthy "${FUGUE_NODE_UPDATE_TASK_ALLOW_RESTART:-false}"; then
    echo "pod capacity change requires allow_restart=true" >&2
    return 1
  fi
  staged="$(mktemp)"
  if ! python3 - "${config}" "${staged}" "${mode}" <<'FUGUE_POD_CAPACITY_PY'
` + podCapacityUIDPython() + `
import pathlib, re, sys
config, staged, mode = map(str, sys.argv[1:])
text = pathlib.Path(config).read_text()
capacity = pod_uid_capacity() if mode == 'resources' else 110
if re.search(r'^[ \t]*-[ \t]*["\x27]?(?:--)?config(?:-dir)?=', text, re.M):
    raise SystemExit('custom kubelet config arguments require explicit capacity review; no configuration changed')
if re.search(r'^[ \t]*kubelet-arg:[ \t]*[^\s#]', text, re.M):
    raise SystemExit('inline kubelet-arg is unsupported; no configuration changed')
lines = text.splitlines()
result, inside, found = [], False, False
for line in lines:
    if re.match(r'^kubelet-arg:\s*(?:#.*)?$', line):
        if found:
            raise SystemExit('duplicate kubelet-arg blocks; no configuration changed')
        found, inside = True, True
        result.extend([line, '  - "max-pods=%d"' % capacity, '  - "pods-per-core=0"'])
        continue
    if inside and line and not line[0].isspace() and not line.lstrip().startswith('#'):
        inside = False
    if inside and re.match(r'^\s*-\s*["\x27]?(?:max-pods|pods-per-core)=', line):
        continue
    result.append(line)
if not found:
    result.extend(['kubelet-arg:', '  - "max-pods=%d"' % capacity, '  - "pods-per-core=0"'])
pathlib.Path(staged).write_text('\n'.join(result) + '\n')
print('Pod capacity mode=%s max_pods=%d; memory requests still enforced' % (mode, capacity))
FUGUE_POD_CAPACITY_PY
  then
    rm -f "${staged}"
    return 1
  fi
  if truthy "${dry_run}"; then
    rm -f "${staged}"
    FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE="Pod capacity plan validated; dry-run"
    return 0
  fi
  if cmp -s "${staged}" "${config}"; then
    rm -f "${staged}"
    FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE="Pod capacity policy already applied"
    return 0
  fi
  repair_guard "pod_capacity_config" 900 1 || { rm -f "${staged}"; return 1; }
  backup="$(mktemp)"
  cp "${config}" "${backup}"
  if ! install -m 0600 "${staged}" "${config}"; then
    rm -f "${staged}" "${backup}"
    return 1
  fi
  rm -f "${staged}"
  if ! reload_pod_capacity_k3s; then
    install -m 0600 "${backup}" "${config}"
    reload_pod_capacity_k3s || true
    rm -f "${backup}"
    repair_record_failure "pod_capacity_config" "L4_guarded_node_repair" "k3s-agent" "capacity reload failed; restored previous configuration"
    return 1
  fi
  rm -f "${backup}"
  repair_record_success "pod_capacity_config" "L4_guarded_node_repair" "k3s-agent"
  FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE="Pod capacity policy applied; kubelet restarted; verify Node allocatable pods"
}
`
}

// Mirrors Kubernetes userns allocation validation. The usable UID range is a
// physical resource, not an administrative Pod quota. Reserving the first ID
// block leaves 65,535 slots on a default host, rather than an invalid int32 max.
func podCapacityUIDPython() string {
	return `import json, os, pathlib, pwd, shutil, subprocess

def uid_capacity(first, length, per_pod):
    if per_pod < 65536 or per_pod % 65536 or first < per_pod or first % per_pod or length % per_pod or first + length > 2**32:
        raise ValueError('invalid kubelet UID/GID allocation range')
    capacity = length // per_pod
    if capacity < 1:
        raise ValueError('kubelet UID/GID allocation range has no Pod capacity')
    return min(capacity, 2147483647)

def pod_uid_capacity():
    per_pod = 65536
    directory = pathlib.Path(os.environ.get('FUGUE_KUBELET_CONFIG_DIR', '/var/lib/rancher/k3s/agent/etc/kubelet.conf.d'))
    for path in sorted(directory.glob('*.conf')):
        text = path.read_text()
        if 'idsPerPod' not in text:
            continue
        try:
            config = json.loads(text)
            per_pod = int(config.get('userNamespaces', {}).get('idsPerPod', per_pod))
        except (ValueError, TypeError, AttributeError):
            raise ValueError('custom user namespace config must be valid JSON for capacity preflight')
    first, length = per_pod, 2**32 - per_pod
    try:
        pwd.getpwnam('kubelet')
        has_user = True
    except KeyError:
        has_user = False
    if has_user and shutil.which('getsubids'):
        uids = subprocess.check_output(['getsubids', 'kubelet'], text=True, timeout=10)
        gids = subprocess.check_output(['getsubids', '-g', 'kubelet'], text=True, timeout=10)
        if uids != gids or len(uids.strip().splitlines()) != 1:
            raise ValueError('kubelet subordinate UID/GID mappings differ or are ambiguous')
        fields = uids.split()
        if len(fields) != 4:
            raise ValueError('invalid getsubids output')
        first, length = int(fields[2]), int(fields[3])
    return uid_capacity(first, length, per_pod)
`
}
