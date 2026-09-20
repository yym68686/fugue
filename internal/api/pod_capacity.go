package api

// The policy is an explicit host task, not a side effect of installing a new
// updater. The resulting kubelet configuration survives code release failures.
func podCapacityShellLibrary() string {
	return `
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
import pathlib, re, sys
config, staged, mode = map(str, sys.argv[1:])
text = pathlib.Path(config).read_text()
capacity = 2147483647 if mode == 'resources' else 110
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
  if ! restart_k3s_agent; then
    install -m 0600 "${backup}" "${config}"
    restart_k3s_agent || true
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
